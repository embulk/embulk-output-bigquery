package org.embulk.output;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import com.google.api.client.http.InputStreamContent;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import com.google.common.base.Optional;
import com.google.api.client.util.Base64;
import com.google.common.base.Throwables;
import java.security.GeneralSecurityException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Hex;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;

public class BigqueryWriter
{

    private final Logger log = Exec.getLogger(BigqueryWriter.class);
    private final String project;
    private final String dataset;
    private final String table;
    private final boolean autoCreateTable;
    private final Optional<String> schemaPath;
    private final TableSchema tableSchema;
    private final String sourceFormat;
    private final String fieldDelimiter;
    private final int maxBadrecords;
    private final String encoding;
    private final boolean preventDuplicateInsert;
    private final long jobStatusMaxPollingTime;
    private final long jobStatusPollingInterval;
    private final boolean isSkipJobResultCheck;
    private final boolean ignoreUnknownValues;
    private final boolean allowQuotedNewlines;
    private final Bigquery bigQueryClient;

    public BigqueryWriter(Builder builder) throws FileNotFoundException, IOException, GeneralSecurityException
    {
        this.project = builder.project;
        this.dataset = builder.dataset;
        this.table = builder.table;
        this.autoCreateTable = builder.autoCreateTable;
        this.schemaPath = builder.schemaPath;
        this.sourceFormat = builder.sourceFormat;
        this.fieldDelimiter = builder.fieldDelimiter;
        this.maxBadrecords = builder.maxBadrecords;
        this.encoding = builder.encoding.toUpperCase();
        this.preventDuplicateInsert = builder.preventDuplicateInsert;
        this.jobStatusMaxPollingTime = builder.jobStatusMaxPollingTime;
        this.jobStatusPollingInterval = builder.jobStatusPollingInterval;
        this.isSkipJobResultCheck = builder.isSkipJobResultCheck;
        this.ignoreUnknownValues = builder.ignoreUnknownValues;
        this.allowQuotedNewlines = builder.allowQuotedNewlines;

        BigqueryAuthentication auth = new BigqueryAuthentication(builder.authMethod, builder.serviceAccountEmail, builder.p12KeyFilePath, builder.applicationName);
        this.bigQueryClient = auth.getBigqueryClient();

        checkConfig();
        if (autoCreateTable) {
            this.tableSchema = createTableSchema(builder.schemaPath);
        } else {
            this.tableSchema = null;
        }
    }

    private String getJobStatus(JobReference jobRef) throws JobFailedException
    {
        try {
            Job job = bigQueryClient.jobs().get(project, jobRef.getJobId()).execute();

            ErrorProto fatalError = job.getStatus().getErrorResult();
            if (fatalError != null) {
                throw new JobFailedException(String.format("Job failed. job id:[%s] reason:[%s][%s] status:[FAILED]", jobRef.getJobId(), fatalError.getReason(), fatalError.getMessage()));
            }
            List<ErrorProto> errors = job.getStatus().getErrors();
            if (errors != null) {
                for (ErrorProto error : errors) {
                    log.error(String.format("Error: job id:[%s] reason[%s][%s] location:[%s]", jobRef.getJobId(), error.getReason(), error.getMessage(), error.getLocation()));
                }
            }

            String jobStatus = job.getStatus().getState();
            if (jobStatus.equals("DONE")) {
                JobStatistics statistics = job.getStatistics();
                //log.info(String.format("Job end. create:[%s] end:[%s]", statistics.getCreationTime(), statistics.getEndTime()));
                log.info(String.format("Job statistics [%s]", statistics.getLoad()));
            }
            return jobStatus;
        } catch (IOException ex) {
            log.warn(ex.getMessage());
            return "UNKNOWN";
        }
    }

    private void getJobStatusUntilDone(JobReference jobRef) throws TimeoutException, JobFailedException
    {
        long startTime = System.currentTimeMillis();
        long elapsedTime;

        try {
            while (true) {
                String jobStatus = getJobStatus(jobRef);
                elapsedTime = System.currentTimeMillis() - startTime;
                if (jobStatus.equals("DONE")) {
                    log.info(String.format("Job completed successfully. job id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, "SUCCESS"));
                    break;
                } else if (elapsedTime > jobStatusMaxPollingTime * 1000) {
                    throw new TimeoutException(String.format("Checking job status...Timeout. job id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, "TIMEOUT"));
                } else {
                    log.info(String.format("Checking job status... job id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, jobStatus));
                }
                Thread.sleep(jobStatusPollingInterval * 1000);
            }
        } catch (InterruptedException ex) {
            log.warn(ex.getMessage());
        }
    }

    public void executeLoad(String localFilePath) throws GoogleJsonResponseException, NoSuchAlgorithmException,
            TimeoutException, JobFailedException, IOException
    {
        log.info(String.format("Job preparing... project:%s dataset:%s table:%s", project, dataset, table));

        Job job = new Job();
        JobReference jobRef = new JobReference();
        JobConfiguration jobConfig = new JobConfiguration();
        JobConfigurationLoad loadConfig = new JobConfigurationLoad();
        jobConfig.setLoad(loadConfig);
        job.setConfiguration(jobConfig);

        if (preventDuplicateInsert) {
            String jobId = createJobId(localFilePath);
            jobRef.setJobId(jobId);
            job.setJobReference(jobRef);
        }

        loadConfig.setAllowQuotedNewlines(allowQuotedNewlines);
        loadConfig.setEncoding(encoding);
        loadConfig.setMaxBadRecords(maxBadrecords);
        if (sourceFormat.equals("NEWLINE_DELIMITED_JSON")) {
            loadConfig.setSourceFormat("NEWLINE_DELIMITED_JSON");
        } else {
            loadConfig.setFieldDelimiter(fieldDelimiter);
        }
        loadConfig.setWriteDisposition("WRITE_APPEND");
        if (autoCreateTable) {
            loadConfig.setSchema(tableSchema);
            loadConfig.setCreateDisposition("CREATE_IF_NEEDED");
            log.info(String.format("table:[%s] will be create if not exists", table));
        } else {
            loadConfig.setCreateDisposition("CREATE_NEVER");
        }
        loadConfig.setIgnoreUnknownValues(ignoreUnknownValues);

        loadConfig.setDestinationTable(createTableReference());

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

        try {
            jobRef = insert.execute().getJobReference();
        } catch (IllegalStateException ex) {
            throw new JobFailedException(ex.getMessage());
        }
        log.info(String.format("Job executed. job id:[%s] file:[%s]", jobRef.getJobId(), localFilePath));
        if (isSkipJobResultCheck) {
            log.info(String.format("Skip job status check. job id:[%s]", jobRef.getJobId()));
        } else {
            getJobStatusUntilDone(jobRef);
        }
    }

    private String createJobId(String localFilePath) throws NoSuchAlgorithmException, IOException
    {
        ImmutableList<Object> elements = ImmutableList.of(
                getLocalMd5hash(localFilePath), dataset, table, tableSchema, sourceFormat,
                fieldDelimiter, maxBadrecords, encoding, ignoreUnknownValues, allowQuotedNewlines
        );

        StringBuilder sb = new StringBuilder();
        for (Object element : elements) {
            sb.append(element);
        }

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(new String(sb).getBytes());
        String hash = new String(Hex.encodeHex(digest));
        return "embulk_job_" + hash;
    }

    private TableReference createTableReference()
    {
        return new TableReference()
                .setProjectId(project)
                .setDatasetId(dataset)
                .setTableId(table);
    }

    private TableSchema createTableSchema(Optional<String> schemaPath) throws FileNotFoundException, IOException
    {
        String path = schemaPath.orNull();
        File file = new File(path);
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(file);
            ObjectMapper mapper = new ObjectMapper();
            List<TableFieldSchema> fields = mapper.readValue(stream, new TypeReference<List<TableFieldSchema>>() {});
            TableSchema tableSchema = new TableSchema().setFields(fields);
            return tableSchema;
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    public boolean isExistTable(String tableName) throws IOException
    {
        Tables tableRequest = bigQueryClient.tables();
        try {
            Table tableData = tableRequest.get(project, dataset, tableName).execute();
        } catch (GoogleJsonResponseException ex) {
            return false;
        }
        return true;
    }

    public void checkConfig() throws FileNotFoundException, IOException
    {
        if (autoCreateTable) {
            if (!schemaPath.isPresent()) {
                throw new FileNotFoundException("schema_path is empty");
            } else {
                File file = new File(schemaPath.orNull());
                if (!file.exists()) {
                    throw new FileNotFoundException("Can not load schema file.");
                }
            }
        } else {
            if (!isExistTable(table)) {
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
        } finally {
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
        private String applicationName;
        private String project;
        private String dataset;
        private String table;
        private boolean autoCreateTable;
        private Optional<String> schemaPath;
        private String sourceFormat;
        private String fieldDelimiter;
        private int maxBadrecords;
        private String encoding;
        private boolean preventDuplicateInsert;
        private int jobStatusMaxPollingTime;
        private int jobStatusPollingInterval;
        private boolean isSkipJobResultCheck;
        private boolean ignoreUnknownValues;
        private boolean allowQuotedNewlines;

        public Builder(String authMethod)
        {
            this.authMethod = authMethod;
        }

        public Builder setServiceAccountEmail(Optional<String> serviceAccountEmail)
        {
            this.serviceAccountEmail = serviceAccountEmail;
            return this;
        }

        public Builder setP12KeyFilePath(Optional<String> p12KeyFilePath)
        {
            this.p12KeyFilePath = p12KeyFilePath;
            return this;
        }

        public Builder setApplicationName(String applicationName)
        {
            this.applicationName = applicationName;
            return this;
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

        public Builder setMaxBadrecords(int maxBadrecords)
        {
            this.maxBadrecords = maxBadrecords;
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
        public JobFailedException(String message) {
            super(message);
        }
    }
}