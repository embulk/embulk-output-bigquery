package org.embulk.output;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Hex;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class BigqueryWriter
{
    private final Logger log = Exec.getLogger(BigqueryWriter.class);
    private final String project;
    private final String dataset;
    private final String table;
    private final boolean autoCreateTable;
    private final Optional<String> schemaPath;
    private final Optional<String> templateTable;
    private final TableSchema tableSchema;
    private final String sourceFormat;
    private final String fieldDelimiter;
    private final int maxBadRecords;
    private final String encoding;
    private final boolean preventDuplicateInsert;
    private final long jobStatusMaxPollingTime;
    private final long jobStatusPollingInterval;
    private final boolean isSkipJobResultCheck;
    private final boolean ignoreUnknownValues;
    private final boolean allowQuotedNewlines;
    private final Bigquery bigQueryClient;

    public BigqueryWriter(Builder builder)
            throws IOException, GeneralSecurityException
    {
        this.project = builder.project;
        this.dataset = builder.dataset;
        this.table = builder.table;
        this.autoCreateTable = builder.autoCreateTable;
        this.schemaPath = builder.schemaPath;
        this.templateTable = builder.templateTable;
        this.sourceFormat = builder.sourceFormat.toUpperCase();
        this.fieldDelimiter = builder.fieldDelimiter;
        this.maxBadRecords = builder.maxBadRecords;
        this.encoding = builder.encoding.toUpperCase();
        this.preventDuplicateInsert = builder.preventDuplicateInsert;
        this.jobStatusMaxPollingTime = builder.jobStatusMaxPollingTime;
        this.jobStatusPollingInterval = builder.jobStatusPollingInterval;
        this.isSkipJobResultCheck = builder.isSkipJobResultCheck;
        this.ignoreUnknownValues = builder.ignoreUnknownValues;
        this.allowQuotedNewlines = builder.allowQuotedNewlines;

        BigqueryAuthentication auth = new BigqueryAuthentication(
                builder.authMethod, builder.serviceAccountEmail, builder.p12KeyFilePath,
                builder.jsonKeyFilePath, builder.applicationName
        );
        this.bigQueryClient = auth.getBigqueryClient();

        checkConfig();

        if (autoCreateTable) {
            if (schemaPath.isPresent()) {
                this.tableSchema = createTableSchema();
            }
            else {
               this.tableSchema = fetchTableSchema();
            }
        }
        else {
            this.tableSchema = null;
        }
    }

    private String getJobStatus(String project, JobReference jobRef) throws JobFailedException
    {
        try {
            Job job = bigQueryClient.jobs().get(project, jobRef.getJobId()).execute();

            List<ErrorProto> errors = job.getStatus().getErrors();
            if (errors != null) {
                for (ErrorProto error : errors) {
                    log.error(String.format("Error: reason[%s][%s] location:[%s]", error.getReason(), error.getMessage(), error.getLocation()));
                }
            }

            ErrorProto fatalError = job.getStatus().getErrorResult();
            if (fatalError != null) {
                throw new JobFailedException(String.format("Job failed. job id:[%s] reason:[%s][%s] status:[FAILED]", jobRef.getJobId(), fatalError.getReason(), fatalError.getMessage()));
            }

            String jobStatus = job.getStatus().getState();
            if (jobStatus.equals("DONE")) {
                JobStatistics statistics = job.getStatistics();
                log.info(String.format("Job statistics [%s]", statistics.getLoad()));
            }
            return jobStatus;
        }
        catch (IOException ex) {
            log.warn(ex.getMessage());
            return "UNKNOWN";
        }
    }

    private void getJobStatusUntilDone(String project, JobReference jobRef) throws TimeoutException, JobFailedException
    {
        long startTime = System.currentTimeMillis();
        long elapsedTime;

        try {
            while (true) {
                String jobStatus = getJobStatus(project, jobRef);
                elapsedTime = System.currentTimeMillis() - startTime;
                if (jobStatus.equals("DONE")) {
                    log.info(String.format("Job completed successfully. job id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, "SUCCESS"));
                    break;
                }
                else if (elapsedTime > jobStatusMaxPollingTime * 1000) {
                    throw new TimeoutException(String.format("Checking job status...Timeout. job id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, "TIMEOUT"));
                }
                else {
                    log.info(String.format("Checking job status... job id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, jobStatus));
                }
                Thread.sleep(jobStatusPollingInterval * 1000);
            }
        }
        catch (InterruptedException ex) {
            log.warn(ex.getMessage());
        }
    }

    public void executeLoad(String project, String dataset, String table, String localFilePath)
            throws NoSuchAlgorithmException, TimeoutException, JobFailedException, IOException
    {
        log.info(String.format("Job preparing... project:%s dataset:%s table:%s", project, dataset, table));

        Job job = new Job();
        JobReference jobRef = new JobReference();
        JobConfiguration jobConfig = new JobConfiguration().setLoad(setLoadConfig(project, dataset, table));
        job.setConfiguration(jobConfig);

        if (preventDuplicateInsert) {
            ImmutableList<String> elements = ImmutableList.of(
                    getLocalMd5hash(localFilePath), dataset, table,
                    String.valueOf(tableSchema), sourceFormat, fieldDelimiter, String.valueOf(maxBadRecords),
                    encoding, String.valueOf(ignoreUnknownValues), String.valueOf(allowQuotedNewlines)
            );
            String jobId = createJobId(elements);

            jobRef.setJobId(jobId);
            job.setJobReference(jobRef);
        }

        File file = new File(localFilePath);
        InputStreamContent mediaContent = new InputStreamContent("application/octet-stream",
                new BufferedInputStream(
                        new FileInputStream(file)));
        mediaContent.setLength(file.length());

        Insert insert = bigQueryClient.jobs().insert(project, job, mediaContent);
        insert.setProjectId(project);
        insert.setDisableGZipContent(true);

        // @see https://code.google.com/p/google-api-java-client/wiki/MediaUpload
        UploadProgressListener listner = new UploadProgressListener();
        listner.setFileName(localFilePath);
        insert.getMediaHttpUploader()
                .setProgressListener(listner)
                .setDirectUploadEnabled(false);

        int retryCount = 0;
        int retryLimit = 3;
        int retryWaitSec = 5;
        while (true) {
            try {
                jobRef = insert.execute().getJobReference();
                break;
            }
            catch (java.net.SocketTimeoutException ex) {
                if (retryCount++ >= retryLimit) {
                    throw new JobFailedException(ex.getMessage());
                }
                log.warn(String.format("Retry after %d secs", retryWaitSec), ex);
            }
            catch (IllegalStateException ex) {
                throw new JobFailedException(ex.getMessage());
            }

            try {
                Thread.sleep(retryWaitSec * 1000);
            }
            catch (InterruptedException ignored) { }
        }
        log.info(String.format("Job executed. job id:[%s] file:[%s]", jobRef.getJobId(), localFilePath));
        if (isSkipJobResultCheck) {
            log.info(String.format("Skip job status check. job id:[%s]", jobRef.getJobId()));
        }
        else {
            getJobStatusUntilDone(project, jobRef);
        }
    }

    public void replaceTable(String project, String dataset, String oldTable, String newTable)
            throws TimeoutException, JobFailedException, IOException
    {
        copyTable(project, dataset, newTable, oldTable, false);
    }

    public void copyTable(String project, String dataset, String fromTable, String toTable, boolean append)
            throws TimeoutException, JobFailedException, IOException
    {
        log.info(String.format("Copy Job preparing... project:%s dataset:%s from:%s to:%s", project, dataset, fromTable, toTable));

        Job job = new Job();
        JobReference jobRef = null;
        JobConfiguration jobConfig = new JobConfiguration().setCopy(setCopyConfig(project, dataset, fromTable, toTable, append));
        job.setConfiguration(jobConfig);
        Insert insert = bigQueryClient.jobs().insert(project, job);
        insert.setProjectId(project);
        insert.setDisableGZipContent(true);

        try {
            jobRef = insert.execute().getJobReference();
        }
        catch (IllegalStateException ex) {
            throw new JobFailedException(ex.getMessage());
        }
        log.info(String.format("Job executed. job id:[%s]", jobRef.getJobId()));
        getJobStatusUntilDone(project, jobRef);
    }

    public void deleteTable(String project, String dataset, String table) throws IOException
    {
        try {
            Tables.Delete delete = bigQueryClient.tables().delete(project, dataset, table);
            delete.execute();
            log.info(String.format("Table deleted. project:%s dataset:%s table:%s", delete.getProjectId(), delete.getDatasetId(), delete.getTableId()));
        }
        catch (GoogleJsonResponseException ex) {
            log.warn(ex.getMessage());
        }
    }

    private JobConfigurationLoad setLoadConfig(String project, String dataset, String table)
    {
        JobConfigurationLoad config = new JobConfigurationLoad();
        config.setAllowQuotedNewlines(allowQuotedNewlines)
                .setEncoding(encoding)
                .setMaxBadRecords(maxBadRecords)
                .setSourceFormat(sourceFormat)
                .setIgnoreUnknownValues(ignoreUnknownValues)
                .setDestinationTable(createTableReference(project, dataset, table))
                .setWriteDisposition("WRITE_APPEND");

        if (sourceFormat.equals("CSV")) {
            config.setFieldDelimiter(String.valueOf(fieldDelimiter));
        }
        if (autoCreateTable) {
            config.setSchema(tableSchema);
            config.setCreateDisposition("CREATE_IF_NEEDED");
            log.info(String.format("table:[%s] will be create if not exists", table));
        }
        else {
            config.setCreateDisposition("CREATE_NEVER");
        }
        return config;
    }

    private JobConfigurationTableCopy setCopyConfig(String project, String dataset, String fromTable, String toTable, boolean append)
    {
        JobConfigurationTableCopy config = new JobConfigurationTableCopy();
        config.setSourceTable(createTableReference(project, dataset, fromTable))
                .setDestinationTable(createTableReference(project, dataset, toTable));

        if (append) {
            config.setWriteDisposition("WRITE_APPEND");
        }
        else {
            config.setWriteDisposition("WRITE_TRUNCATE");
        }

        return config;
    }

    private String createJobId(ImmutableList<String> elements) throws NoSuchAlgorithmException, IOException
    {
        StringBuilder sb = new StringBuilder();
        for (String element : elements) {
            sb.append(element);
        }

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(new String(sb).getBytes());
        String hash = new String(Hex.encodeHex(digest));

        StringBuilder jobId = new StringBuilder();
        jobId.append("embulk_job_");
        jobId.append(hash);
        return jobId.toString();
    }

    private TableReference createTableReference(String project, String dataset, String table)
    {
        return new TableReference()
                .setProjectId(project)
                .setDatasetId(dataset)
                .setTableId(table);
    }

    public TableSchema createTableSchema() throws IOException
    {
        String path = schemaPath.orNull();
        File file = new File(path);
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(file);
            ObjectMapper mapper = new ObjectMapper();
            List<TableFieldSchema> fields = mapper.readValue(stream, new TypeReference<List<TableFieldSchema>>() {});
            return new TableSchema().setFields(fields);
        }
        finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    public TableSchema fetchTableSchema() throws IOException
    {
        String fetchTarget = templateTable.orNull();
        log.info(String.format("Fetch table schema from project:%s dataset:%s table:%s", project, dataset, fetchTarget));
        Tables tableRequest = bigQueryClient.tables();
        Table tableData = tableRequest.get(project, dataset, fetchTarget).execute();
        return tableData.getSchema();
    }

    public boolean isExistTable(String project, String dataset, String table) throws IOException
    {
        Tables tableRequest = bigQueryClient.tables();
        try {
            Table tableData = tableRequest.get(project, dataset, table).execute();
        }
        catch (GoogleJsonResponseException ex) {
            return false;
        }
        return true;
    }

    public void checkConfig() throws IOException
    {
        if (autoCreateTable) {
            if (schemaPath.isPresent()) {
                File file = new File(schemaPath.orNull());
                if (!file.exists()) {
                    throw new FileNotFoundException("Can not load schema file.");
                }
            }
            else if (!templateTable.isPresent()) {
                throw new FileNotFoundException("schema_file or template_table must be present");
            }
        }
        else {
            if (!isExistTable(project, dataset, table)) {
                throw new IOException(String.format("table [%s] is not exists", table));
            }
        }
    }

    private String getLocalMd5hash(String filePath) throws NoSuchAlgorithmException, IOException
    {
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(filePath);
            MessageDigest digest = MessageDigest.getInstance("MD5");

            byte[] bytesBuffer = new byte[1024];
            int bytesRead = -1;

            while ((bytesRead = stream.read(bytesBuffer)) != -1) {
                digest.update(bytesBuffer, 0, bytesRead);
            }
            byte[] hashedBytes = digest.digest();

            byte[] encoded = (hashedBytes);
            return new String(encoded);
        }
        finally {
            stream.close();
        }
    }

    private class UploadProgressListener implements MediaHttpUploaderProgressListener
    {
        private String fileName;

        @Override
        public void progressChanged(MediaHttpUploader uploader) throws IOException
        {
            switch (uploader.getUploadState()) {
                case INITIATION_STARTED:
                    log.info(String.format("Upload start [%s]", fileName));
                    break;
                case INITIATION_COMPLETE:
                    //log.info(String.format("Upload initiation completed file [%s]", fileName));
                    break;
                case MEDIA_IN_PROGRESS:
                    log.debug(String.format("Uploading [%s] progress %3.0f", fileName, uploader.getProgress() * 100) + "%");
                    break;
                case MEDIA_COMPLETE:
                    log.info(String.format("Upload completed [%s]", fileName));
            }
        }

        public void setFileName(String fileName)
        {
            this.fileName = fileName;
        }
    }

    public static class Builder
    {
        private final String authMethod;
        private Optional<String> serviceAccountEmail;
        private Optional<String> p12KeyFilePath;
        private Optional<String> jsonKeyFilePath;
        private String applicationName;
        private String project;
        private String dataset;
        private String table;
        private boolean autoCreateTable;
        private Optional<String> schemaPath;
        private Optional<String> templateTable;
        private String sourceFormat;
        private String fieldDelimiter;
        private int maxBadRecords;
        private String encoding;
        private boolean preventDuplicateInsert;
        private int jobStatusMaxPollingTime;
        private int jobStatusPollingInterval;
        private boolean isSkipJobResultCheck;
        private boolean ignoreUnknownValues;
        private boolean allowQuotedNewlines;

        public Builder(String authMethod, Optional<String> serviceAccountEmail, Optional<String> p12KeyFilePath,
                Optional<String> jsonKeyFilePath, String applicationName)
        {
            this.authMethod = authMethod;
            this.serviceAccountEmail = serviceAccountEmail;
            this.p12KeyFilePath = p12KeyFilePath;
            this.jsonKeyFilePath = jsonKeyFilePath;
            this.applicationName = applicationName;
        }

        public Builder setProject(String project)
        {
            this.project = project;
            return this;
        }

        public Builder setDataset(String dataset)
        {
            this.dataset = dataset;
            return this;
        }

        public Builder setTable(String table)
        {
            this.table = table;
            return this;
        }

        public Builder setAutoCreateTable(boolean autoCreateTable)
        {
            this.autoCreateTable = autoCreateTable;
            return this;
        }

        public Builder setSchemaPath(Optional<String> schemaPath)
        {
            this.schemaPath = schemaPath;
            return this;
        }

        public Builder setTemplateTable(Optional<String> templateTable)
        {
            this.templateTable = templateTable;
            return this;
        }

        public Builder setSourceFormat(String sourceFormat)
        {
            this.sourceFormat = sourceFormat;
            return this;
        }

        public Builder setFieldDelimiter(String fieldDelimiter)
        {
            this.fieldDelimiter = fieldDelimiter;
            return this;
        }

        public Builder setMaxBadRecords(int maxBadRecords)
        {
            this.maxBadRecords = maxBadRecords;
            return this;
        }

        public Builder setEncoding(String encoding)
        {
            this.encoding = encoding;
            return this;
        }

        public Builder setPreventDuplicateInsert(boolean preventDuplicateInsert)
        {
            this.preventDuplicateInsert = preventDuplicateInsert;
            return this;
        }

        public Builder setJobStatusMaxPollingTime(int jobStatusMaxPollingTime)
        {
            this.jobStatusMaxPollingTime = jobStatusMaxPollingTime;
            return this;
        }

        public Builder setJobStatusPollingInterval(int jobStatusPollingInterval)
        {
            this.jobStatusPollingInterval = jobStatusPollingInterval;
            return this;
        }

        public Builder setIsSkipJobResultCheck(boolean isSkipJobResultCheck)
        {
            this.isSkipJobResultCheck = isSkipJobResultCheck;
            return this;
        }

        public Builder setIgnoreUnknownValues(boolean ignoreUnknownValues)
        {
            this.ignoreUnknownValues = ignoreUnknownValues;
            return this;
        }

        public Builder setAllowQuotedNewlines(boolean allowQuotedNewlines)
        {
            this.allowQuotedNewlines = allowQuotedNewlines;
            return this;
        }

        public BigqueryWriter build() throws IOException, GeneralSecurityException
        {
            return new BigqueryWriter(this);
        }
    }

    public class JobFailedException extends RuntimeException
    {
        public JobFailedException(String message)
        {
            super(message);
        }
    }
}
