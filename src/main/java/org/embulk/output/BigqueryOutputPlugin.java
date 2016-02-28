package org.embulk.output;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.unit.LocalFile;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class BigqueryOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("auth_method")
        @ConfigDefault("\"private_key\"")
        AuthMethod getAuthMethod();

        @Config("service_account_email")
        @ConfigDefault("null")
        Optional<String> getServiceAccountEmail();

        // kept for backward compatibility
        @Config("p12_keyfile_path")
        @ConfigDefault("null")
        Optional<String> getP12KeyfilePath();

        @Config("p12_keyfile")
        @ConfigDefault("null")
        Optional<LocalFile> getP12Keyfile();
        void setP12Keyfile(Optional<LocalFile> p12Keyfile);

        @Config("json_keyfile")
        @ConfigDefault("null")
        Optional<LocalFile> getJsonKeyfile();

        @Config("application_name")
        @ConfigDefault("\"Embulk BigQuery plugin\"")
        String getApplicationName();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d\"")
        String getSequenceFormat();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("source_format")
        @ConfigDefault("\"CSV\"")
        SourceFormat getSourceFormat();

        @Config("field_delimiter")
        @ConfigDefault("\",\"")
        char getFieldDelimiter();

        @Config("max_bad_records")
        @ConfigDefault("0")
        int getMaxBadrecords();

        @Config("encoding")
        @ConfigDefault("\"UTF-8\"")
        Charset getEncoding();

        @Config("delete_from_local_when_job_end")
        @ConfigDefault("false")
        boolean getDeleteFromLocalWhenJobEnd();

        @Config("project")
        String getProject();

        @Config("dataset")
        String getDataset();

        @Config("table")
        String getTable();

        @Config("auto_create_table")
        @ConfigDefault("false")
        boolean getAutoCreateTable();

        // kept for backward compatibility
        @Config("schema_path")
        @ConfigDefault("null")
        Optional<String> getSchemaPath();

        @Config("schema_file")
        @ConfigDefault("null")
        Optional<LocalFile> getSchemaFile();
        void setSchemaFile(Optional<LocalFile> schemaFile);

        @Config("template_table")
        @ConfigDefault("null")
        Optional<String> getTemplateTable();

        @Config("prevent_duplicate_insert")
        @ConfigDefault("false")
        boolean getPreventDuplicateInsert();

        @Config("job_status_max_polling_time")
        @ConfigDefault("3600")
        int getJobStatusMaxPollingTime();

        @Config("job_status_polling_interval")
        @ConfigDefault("10")
        int getJobStatusPollingInterval();

        @Config("is_skip_job_result_check")
        @ConfigDefault("false")
        boolean getIsSkipJobResultCheck();

        @Config("ignore_unknown_values")
        @ConfigDefault("false")
        boolean getIgnoreUnknownValues();

        @Config("allow_quoted_newlines")
        @ConfigDefault("false")
        boolean getAllowQuotedNewlines();

        @Config("mode")
        @ConfigDefault("\"append\"")
        Mode getMode();
    }

    private final Logger log = Exec.getLogger(BigqueryOutputPlugin.class);
    private static final String temporaryTableSuffix = Long.toString(System.currentTimeMillis());
    private static BigqueryWriter bigQueryWriter;
    private static final List<File> uploadFiles = Collections.synchronizedList(new ArrayList<File>());

    private static class UploadWorker implements Callable<Throwable>
    {
        private final String project;
        private final String dataset;
        private final String table;
        private final boolean deleteFromLocalWhenJobEnd;
        private final File uploadFile;
        private final Future<Throwable> future;
        private final Logger log = Exec.getLogger(BigqueryOutputPlugin.class);

        public UploadWorker(String project, String dataset, String table, File uploadFile, boolean deleteFromLocalWhenJobEnd, ExecutorService executor)
        {
            this.project = project;
            this.dataset = dataset;
            this.table = table;
            this.deleteFromLocalWhenJobEnd = deleteFromLocalWhenJobEnd;
            this.uploadFile = uploadFile;
            this.future = executor.submit(this);
        }

        public synchronized Throwable call()
        {
            try {
                bigQueryWriter.executeLoad(project, dataset, table, this.uploadFile.getPath());

                if (deleteFromLocalWhenJobEnd) {
                    log.info(String.format("Delete local file [%s]", this.uploadFile.getPath()));
                    this.uploadFile.delete();
                }

                return null;
            }
            catch (NoSuchAlgorithmException | TimeoutException | BigqueryWriter.JobFailedException | IOException ex) {
                log.error(ex.getMessage());
                throw Throwables.propagate(ex);
            }
        }

        public Throwable join()
                throws InterruptedException
        {
            try {
                return future.get();
            }
            catch (ExecutionException ex) {
                return ex.getCause();
            }
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
                                  FileOutputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        if (task.getP12KeyfilePath().isPresent()) {
            if (task.getP12Keyfile().isPresent()) {
                throw new ConfigException("Setting both p12_keyfile_path and p12_keyfile is invalid");
            }
            try {
                task.setP12Keyfile(Optional.of(LocalFile.of(task.getP12KeyfilePath().get())));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        if (task.getSchemaPath().isPresent()) {
            if (task.getSchemaFile().isPresent()) {
                throw new ConfigException("Setting both p12_keyfile_path and p12_keyfile is invalid");
            }
            try {
                task.setSchemaFile(Optional.of(LocalFile.of(task.getSchemaPath().get())));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        if (task.getAuthMethod().getString().equals("json_key")) {
            if (!task.getJsonKeyfile().isPresent()) {
                throw new ConfigException("If auth_method is json_key, you have to set json_keyfile");
            }
        }
        else if (task.getAuthMethod().getString().equals("private_key")) {
            if (!task.getP12Keyfile().isPresent() || !task.getServiceAccountEmail().isPresent()) {
                throw new ConfigException("If auth_method is private_key, you have to set both service_account_email and p12_keyfile");
            }
        }

        if (task.getMode().isReplaceMode()) {
            if (task.getIsSkipJobResultCheck()) {
                throw new ConfigException("If mode is replace or replace_backup, is_skip_job_result_check must be false");
            }
        }

        if (task.getMode().isDeleteInAdvance()) {
            if (!task.getAutoCreateTable()) {
                throw new ConfigException("If mode is delete_in_advance, auto_create_table must be true");
            }
        }

        try {
            bigQueryWriter = new BigqueryWriter.Builder(
                    task.getAuthMethod().getString(),
                    task.getServiceAccountEmail(),
                    task.getP12Keyfile().transform(localFileToPathString()),
                    task.getJsonKeyfile().transform(localFileToPathString()),
                    task.getApplicationName())
                    .setProject(task.getProject())
                    .setDataset(task.getDataset())
                    .setTable(task.getTable())
                    .setAutoCreateTable(task.getAutoCreateTable())
                    .setSchemaPath(task.getSchemaFile().transform(localFileToPathString()))
                    .setTemplateTable(task.getTemplateTable())
                    .setSourceFormat(task.getSourceFormat().getString())
                    .setFieldDelimiter(String.valueOf(task.getFieldDelimiter()))
                    .setMaxBadRecords(task.getMaxBadrecords())
                    .setEncoding(String.valueOf(task.getEncoding()))
                    .setPreventDuplicateInsert(task.getPreventDuplicateInsert())
                    .setJobStatusMaxPollingTime(task.getJobStatusMaxPollingTime())
                    .setJobStatusPollingInterval(task.getJobStatusPollingInterval())
                    .setIsSkipJobResultCheck(task.getIsSkipJobResultCheck())
                    .setIgnoreUnknownValues(task.getIgnoreUnknownValues())
                    .setAllowQuotedNewlines(task.getAllowQuotedNewlines())
                    .build();
        }
        catch (IOException | GeneralSecurityException ex) {
            throw new ConfigException(ex);
        }
        // non-retryable (non-idempotent) output:
        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileOutputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        Mode mode = task.getMode();
        String project = task.getProject();
        String dataset = task.getDataset();
        String tableName = task.getTable();
        boolean deleteFromLocalWhenJobEnd = task.getDeleteFromLocalWhenJobEnd();

        if (mode == Mode.delete_in_advance) {
            try {
                bigQueryWriter.deleteTable(project, dataset, generateTableName(tableName));
            }
            catch (IOException ex) {
                log.warn(ex.getMessage());
            }
        }

        control.run(taskSource);

        if (!uploadFiles.isEmpty()) {
            String uploadTable = task.getMode().isReplaceMode() ?
                    generateTemporaryTableName(task.getTable()) : generateTableName(task.getTable());
            upload(project, dataset, uploadTable, deleteFromLocalWhenJobEnd);

            if (mode.isReplaceMode()) {
                replaceTable(project, dataset, tableName, mode);
            }
        }

        return Exec.newConfigDiff();
    }

    private void upload(String project, String dataset, String tableName, boolean deleteFromLocalWhenJobEnd)
    {
        ExecutorService uploadExecutor = java.util.concurrent.Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                        .setNameFormat("embulk-output-bigquery-upload-%d")
                        .setDaemon(true)
                        .build());
        ArrayList<UploadWorker> uploadWorkers = new ArrayList<>();
        for (File uploadFile : uploadFiles) {
            UploadWorker worker = new UploadWorker(project, dataset, tableName, uploadFile, deleteFromLocalWhenJobEnd, uploadExecutor);
            uploadWorkers.add(worker);
        }

        for (UploadWorker worker : uploadWorkers) {
            Throwable error = null;
            try {
                error = worker.join();
            }
            catch (InterruptedException ex) {
                error = ex;
            }
            if (error != null) {
                throw Throwables.propagate(error);
            }
        }
    }

    private void replaceTable(String project, String dataset, String tableName, Mode mode)
    {
        String targetTable = generateTableName(tableName);
        String temporaryTable = generateTemporaryTableName(tableName);
        try {
            if (mode == Mode.replace_backup && bigQueryWriter.isExistTable(project, dataset, targetTable)) {
                bigQueryWriter.replaceTable(project, dataset, targetTable + "_old", targetTable);
            }
            bigQueryWriter.replaceTable(project, dataset, targetTable, temporaryTable);
        }
        catch (TimeoutException | BigqueryWriter.JobFailedException | IOException ex) {
            log.error(ex.getMessage());
            throw Throwables.propagate(ex);
        }
        finally {
            try {
                bigQueryWriter.deleteTable(project, dataset, temporaryTable);
            }
            catch (IOException ex) {
                log.warn(ex.getMessage());
            }
        }
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    private Function<LocalFile, String> localFileToPathString()
    {
        return new Function<LocalFile, String>()
        {
            public String apply(LocalFile file)
            {
                return file.getPath().toString();
            }
        };
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final String pathPrefix = task.getPathPrefix();
        final String sequenceFormat = task.getSequenceFormat();
        final String pathSuffix = task.getFileNameExtension();

        return new TransactionalFileOutput() {
            private int fileIndex = 0;
            private BufferedOutputStream output = null;
            private File file;
            private String filePath;

            public void nextFile()
            {
                closeFile();

                try {
                    String suffix = pathSuffix;
                    if (!suffix.startsWith(".")) {
                        suffix = "." + suffix;
                    }
                    filePath = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + suffix;
                    file = new File(filePath);
                    uploadFiles.add(file);

                    String parentPath = file.getParent();
                    File dir = new File(parentPath);
                    if (!dir.exists()) {
                        dir.mkdir();
                    }
                    log.info(String.format("Writing file [%s]", filePath));
                    output = new BufferedOutputStream(new FileOutputStream(filePath));
                }
                catch (FileNotFoundException ex) {
                    throw Throwables.propagate(ex);
                }
                fileIndex++;
            }

            private void closeFile()
            {
                if (output != null) {
                    try {
                        output.close();
                    }
                    catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }

            public void add(Buffer buffer)
            {
                try {
                    output.write(buffer.array(), buffer.offset(), buffer.limit());
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
                finally {
                    buffer.release();
                }
            }

            public void finish()
            {
                closeFile();
            }

            public void close()
            {
                closeFile();
            }

            public void abort()
            {
            }

            public TaskReport commit()
            {
                TaskReport report = Exec.newTaskReport();
                return report;
            }
        };
    }

    // Parse like "table_%Y_%m"(include pattern or not) format using Java is difficult. So use jRuby.
    public String generateTableName(String tableName)
    {
        ScriptingContainer jruby = new ScriptingContainer();
        Object result = jruby.runScriptlet("Time.now.strftime('" + tableName + "')");

        return result.toString();
    }

    public String generateTemporaryTableName(String tableName)
    {
        return generateTableName(tableName) + temporaryTableSuffix;
    }

    public enum SourceFormat
    {
        CSV("CSV"),
        NEWLINE_DELIMITED_JSON("NEWLINE_DELIMITED_JSON");

        private final String string;

        SourceFormat(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }
    }

    public enum AuthMethod
    {
        private_key("private_key"),
        compute_engine("compute_engine"),
        json_key("json_key");

        private final String string;

        AuthMethod(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }
    }

    public enum Mode
    {
        append("append"),
        delete_in_advance("delete_in_advance") {
            @Override
            public boolean isDeleteInAdvance()
            {
                return true;
            }
        },
        replace("replace") {
            @Override
            public boolean isReplaceMode()
            {
                return true;
            }
        },
        replace_backup("replace_backup") {
            @Override
            public boolean isReplaceMode()
            {
                return true;
            }
        };

        private final String string;

        Mode(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }
        public boolean isReplaceMode()
        {
            return false;
        }
        public boolean isDeleteInAdvance()
        {
            return false;
        }
    }
}
