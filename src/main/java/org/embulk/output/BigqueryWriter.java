package org.embulk.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import java.security.GeneralSecurityException;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

public class BigqueryWriter
{

    private final Logger log = Exec.getLogger(BigqueryWriter.class);
    private final String project;
    private final String dataset;
    private final String table;
    private final boolean autoCreateTable;
    private final Optional<String> schemaPath;
    private final String bucket;
    private final String sourceFormat;
    private final String fieldDelimiter;
    private final int maxBadrecords;
    private final long jobStatusMaxPollingTime;
    private final long jobStatusPollingInterval;
    private final boolean isSkipJobResultCheck;
    private final Bigquery bigQueryClient;
    private final EmbulkBigqueryTask writerTask;

    public BigqueryWriter(Builder builder) throws IOException, GeneralSecurityException
    {
        this.project = builder.project;
        this.dataset = builder.dataset;
        this.table = builder.table;
        this.autoCreateTable = builder.autoCreateTable;
        this.schemaPath = builder.schemaPath;
        this.bucket = builder.bucket;
        this.sourceFormat = builder.sourceFormat.toUpperCase();
        this.fieldDelimiter = builder.fieldDelimiter;
        this.maxBadrecords = builder.maxBadrecords;
        this.jobStatusMaxPollingTime = builder.jobStatusMaxPollingTime;
        this.jobStatusPollingInterval = builder.jobStatusPollingInterval;
        this.isSkipJobResultCheck = builder.isSkipJobResultCheck;

        BigqueryAuthentication auth = new BigqueryAuthentication(builder.serviceAccountEmail, builder.p12KeyFilePath, builder.applicationName);
        this.bigQueryClient = auth.getBigqueryClient();
        this.writerTask = new EmbulkBigqueryTask();
    }

    private String getJobStatus(JobReference jobRef) throws JobFailedException
    {
        try {
            Job job = bigQueryClient.jobs().get(project, jobRef.getJobId()).execute();
            if (job.getStatus().getErrorResult() != null) {
                throw new JobFailedException(String.format("Job failed. job id:[%s] reason:[%s] status:[FAILED]", jobRef.getJobId(), job.getStatus().getErrorResult().getMessage()));
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
                    log.info(String.format("Job completed successfully. job_id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, "SUCCESS"));
                    break;
                } else if (elapsedTime > jobStatusMaxPollingTime * 1000) {
                    throw new TimeoutException(String.format("Checking job status...Timeout. job_id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, "TIMEOUT"));
                } else {
                    log.info(String.format("Checking job status... job_id:[%s] elapsed_time:%dms status:[%s]", jobRef.getJobId(), elapsedTime, jobStatus));
                }
                Thread.sleep(jobStatusPollingInterval * 1000);
            }
        } catch (InterruptedException ex) {
            log.warn(ex.getMessage());
        }
    }

    public void executeJob() throws IOException, TimeoutException, JobFailedException
    {
        // TODO: refactor
        ArrayList<ArrayList<HashMap<String, String>>> taskList = writerTask.createJobList();
        for (ArrayList<HashMap<String, String>> task : taskList) {
            Job job = createJob(task);
            // TODO: multi-threading
            new EmbulkBigqueryJob(job).call();
        }
    }

    private Job createJob(ArrayList<HashMap<String, String>> task)
    {
        log.info(String.format("Job preparing... project:%s dataset:%s table:%s", project, dataset, table));

        Job job = new Job();
        JobConfiguration jobConfig = new JobConfiguration();
        JobConfigurationLoad loadConfig = new JobConfigurationLoad();
        jobConfig.setLoad(loadConfig);
        job.setConfiguration(jobConfig);

        loadConfig.setAllowQuotedNewlines(false);
        if (sourceFormat.equals("NEWLINE_DELIMITED_JSON")) {
            loadConfig.setSourceFormat("NEWLINE_DELIMITED_JSON");
        } else {
            loadConfig.setFieldDelimiter(fieldDelimiter);
        }
        if (autoCreateTable) {
            loadConfig.setSchema(getTableSchema());
            loadConfig.setWriteDisposition("WRITE_EMPTY");
            loadConfig.setCreateDisposition("CREATE_IF_NEEDED");
            log.info(String.format("table:[%s] will be create.", table));
        } else {
            loadConfig.setWriteDisposition("WRITE_APPEND");
            loadConfig.setCreateDisposition("CREATE_NEVER");
        }
        loadConfig.setMaxBadRecords(maxBadrecords);

        List<String> sources = new ArrayList<String>();
        for (HashMap<String, String> file : task) {
            String sourceFile;
            String remotePath = getRemotePath(file.get("remote_path"), file.get("file_name"));
            sourceFile = "gs://" + remotePath;
            log.info(String.format("Add source file to job [%s]", sourceFile));
            sources.add(sourceFile);
        }
        loadConfig.setSourceUris(sources);
        loadConfig.setDestinationTable(getTableReference());

        return job;
    }

    private TableReference getTableReference()
    {
        return new TableReference()
                .setProjectId(project)
                .setDatasetId(dataset)
                .setTableId(table);
    }

    private TableSchema getTableSchema()
    {
        TableSchema tableSchema = new TableSchema();
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        TableFieldSchema tableField;
        // TODO import from json file
        /*
        for () {
            tableField = new TableFieldSchema()
                    .setName(name)
                    .setType(type);
            fields.add(tableField);
        }
        */

        tableSchema.setFields(fields);
        return tableSchema;
    }

    private String getRemotePath(String remotePath, String fileName)
    {
        String[] pathList = StringUtils.split(remotePath, '/');
        String path;
        if (remotePath.isEmpty()) {
            path = bucket + "/" + fileName;
        } else {
            path = bucket + "/" + StringUtils.join(pathList) + "/" + fileName;
        }
        return path;
    }

    public void addTask(Optional<String> remotePath, String fileName, long fileSize)
    {
        writerTask.addTaskFile(remotePath, fileName, fileSize);
    }

    public ArrayList<HashMap<String, String>> getFileList()
    {
        return writerTask.getFileList();
    }

    private class EmbulkBigqueryJob implements Callable<Void>
    {
        private final Job job;

        public EmbulkBigqueryJob(Job job)
        {
            this.job = job;
        }

        public Void call() throws IOException, TimeoutException, JobFailedException
        {
            Insert insert = bigQueryClient.jobs().insert(project, job);
            insert.setProjectId(project);
            JobReference jobRef = insert.execute().getJobReference();
            log.info(String.format("Job executed. job id:[%s]", jobRef.getJobId()));
            if (isSkipJobResultCheck) {
                log.info(String.format("Skip job status check. job id:[%s]", jobRef.getJobId()));
            } else {
                getJobStatusUntilDone(jobRef);
            }
            return null;
        }
    }

    private class EmbulkBigqueryTask
    {
        // https://cloud.google.com/bigquery/loading-data-into-bigquery#quota
        private final long MAX_SIZE_PER_LOAD_JOB = 1000 * 1024 * 1024 * 1024L; // 1TB
        private final int MAX_NUMBER_OF_FILES_PER_LOAD_JOB = 10000;

        private final ArrayList<HashMap<String, String>> taskList = new ArrayList<HashMap<String, String>>();
        private final ArrayList<ArrayList<HashMap<String, String>>> jobList = new ArrayList<ArrayList<HashMap<String, String>>>();

        public void addTaskFile(Optional<String> remotePath, String fileName, long fileSize)
        {
            HashMap<String, String> task = new HashMap<String, String>();
            if (remotePath.isPresent()) {
                task.put("remote_path", remotePath.get());
            } else {
                task.put("remote_path", "");
            }
            task.put("file_name", fileName);
            task.put("file_size", String.valueOf(fileSize));
            taskList.add(task);
        }

        public ArrayList<ArrayList<HashMap<String, String>>> createJobList()
        {
            long currentBundleSize = 0;
            int currentFileCount = 0;
            ArrayList<HashMap<String, String>> job = new ArrayList<HashMap<String, String>>();
            for (HashMap<String, String> task : taskList) {
                boolean isNeedNextJobList = false;
                long fileSize = Long.valueOf(task.get("file_size")).longValue();

                if (currentBundleSize + fileSize > MAX_SIZE_PER_LOAD_JOB) {
                    isNeedNextJobList = true;
                }

                if (currentFileCount >= MAX_NUMBER_OF_FILES_PER_LOAD_JOB) {
                    isNeedNextJobList = true;
                }

                if (isNeedNextJobList) {
                    jobList.add(job);
                    job = new ArrayList<HashMap<String, String>>();
                    job.add(task);
                    currentBundleSize = 0;
                } else {
                    job.add(task);
                }
                currentBundleSize += fileSize;
                currentFileCount++;

                log.debug(String.format("currentBundleSize:%s currentFileCount:%s", currentBundleSize, currentFileCount));
                log.debug(String.format("fileSize:%s, MAX_SIZE_PER_LOAD_JOB:%s MAX_NUMBER_OF_FILES_PER_LOAD_JOB:%s",
                        fileSize, MAX_SIZE_PER_LOAD_JOB, MAX_NUMBER_OF_FILES_PER_LOAD_JOB));

            }
            if (job.size() > 0) {
                jobList.add(job);
            }
            return jobList;
        }

        public ArrayList<HashMap<String, String>> getFileList()
        {
            return taskList;
        }
    }

    public static class Builder
    {
        private final String serviceAccountEmail;
        private String p12KeyFilePath;
        private String applicationName;
        private String project;
        private String dataset;
        private String table;
        private boolean autoCreateTable;
        private Optional<String> schemaPath;
        private String bucket;
        private String sourceFormat;
        private String fieldDelimiter;
        private int maxBadrecords;
        private int jobStatusMaxPollingTime;
        private int jobStatusPollingInterval;
        private boolean isSkipJobResultCheck;


        public Builder(String serviceAccountEmail)
        {
            this.serviceAccountEmail = serviceAccountEmail;
        }

        public Builder setP12KeyFilePath(String p12KeyFilePath)
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

        public Builder setBucket(String bucket)
        {
            this.bucket = bucket;
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

        public BigqueryWriter build() throws IOException, GeneralSecurityException
        {
            return new BigqueryWriter(this);
        }
    }

    public class JobFailedException extends Exception
    {
        public JobFailedException(String message) {
            super(message);
        }
    }
}