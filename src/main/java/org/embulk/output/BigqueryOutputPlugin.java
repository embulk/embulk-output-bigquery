package org.embulk.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import java.security.GeneralSecurityException;
import org.jruby.embed.ScriptingContainer;

import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigDiff;
import org.embulk.config.CommitReport;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.Buffer;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.Exec;

import org.slf4j.Logger;

public class BigqueryOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("auth_method")
        @ConfigDefault("\"private_key\"")
        public AuthMethod getAuthMethod();

        @Config("service_account_email")
        @ConfigDefault("null")
        public Optional<String> getServiceAccountEmail();

        // kept for backward compatibility
        @Config("p12_keyfile_path")
        @ConfigDefault("null")
        public Optional<String> getP12KeyfilePath();

        @Config("p12_keyfile")
        @ConfigDefault("null")
        public Optional<LocalFile> getP12Keyfile();
        public void setP12Keyfile(Optional<LocalFile> p12Keyfile);

        @Config("application_name")
        @ConfigDefault("\"Embulk BigQuery plugin\"")
        public String getApplicationName();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d\"")
        public String getSequenceFormat();

        @Config("file_ext")
        public String getFileNameExtension();

        @Config("source_format")
        @ConfigDefault("\"CSV\"")
        public SourceFormat getSourceFormat();

        @Config("field_delimiter")
        @ConfigDefault("\",\"")
        public char getFieldDelimiter();

        @Config("max_bad_records")
        @ConfigDefault("0")
        public int getMaxBadrecords();

        @Config("encoding")
        @ConfigDefault("\"UTF-8\"")
        public Charset getEncoding();

        @Config("delete_from_local_when_job_end")
        @ConfigDefault("false")
        public boolean getDeleteFromLocalWhenJobEnd();

        @Config("project")
        public String getProject();

        @Config("dataset")
        public String getDataset();

        @Config("table")
        public String getTable();

        @Config("auto_create_table")
        @ConfigDefault("false")
        public boolean getAutoCreateTable();

        // kept for backward compatibility
        @Config("schema_path")
        @ConfigDefault("null")
        public Optional<String> getSchemaPath();

        @Config("schema_file")
        @ConfigDefault("null")
        public Optional<LocalFile> getSchemaFile();
        public void setSchemaFile(Optional<LocalFile> schemaFile);

        @Config("prevent_duplicate_insert")
        @ConfigDefault("false")
        public boolean getPreventDuplicateInsert();

        @Config("job_status_max_polling_time")
        @ConfigDefault("3600")
        public int getJobStatusMaxPollingTime();

        @Config("job_status_polling_interval")
        @ConfigDefault("10")
        public int getJobStatusPollingInterval();

        @Config("is_skip_job_result_check")
        @ConfigDefault("false")
        public boolean getIsSkipJobResultCheck();

        @Config("ignore_unknown_values")
        @ConfigDefault("false")
        public boolean getIgnoreUnknownValues();

        @Config("allow_quoted_newlines")
        @ConfigDefault("false")
        public boolean getAllowQuotedNewlines();
    }

    private final Logger log = Exec.getLogger(BigqueryOutputPlugin.class);
    private static BigqueryWriter bigQueryWriter;

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
            } catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        if (task.getSchemaPath().isPresent()) {
            if (task.getSchemaFile().isPresent()) {
                throw new ConfigException("Setting both p12_keyfile_path and p12_keyfile is invalid");
            }
            try {
                task.setSchemaFile(Optional.of(LocalFile.of(task.getSchemaPath().get())));
            } catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        try {
            bigQueryWriter = new BigqueryWriter.Builder (
                    task.getAuthMethod().getString(),
                    task.getServiceAccountEmail(),
                    task.getP12Keyfile().transform(localFileToPathString()),
                    task.getApplicationName())
                    .setAutoCreateTable(task.getAutoCreateTable())
                    .setSchemaPath(task.getSchemaFile().transform(localFileToPathString()))
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

            bigQueryWriter.checkConfig(task.getProject(), task.getDataset(), task.getTable());

        } catch (IOException | GeneralSecurityException ex) {
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
        control.run(taskSource);

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<CommitReport> successCommitReports)
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
            private final String project = task.getProject();
            private final String dataset = task.getDataset();
            private final String table = generateTableName(task.getTable());
            private final boolean deleteFromLocalWhenJobEnd = task.getDeleteFromLocalWhenJobEnd();

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

                    String parentPath = file.getParent();
                    File dir = new File(parentPath);
                    if (!dir.exists()) {
                        dir.mkdir();
                    }
                    log.info(String.format("Writing file [%s]", filePath));
                    output = new BufferedOutputStream(new FileOutputStream(filePath));
                } catch (FileNotFoundException ex) {
                    throw Throwables.propagate(ex);
                }
                fileIndex++;
            }

            private void closeFile()
            {
                if (output != null) {
                    try {
                        output.close();
                    } catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }

            public void add(Buffer buffer)
            {
                try {
                    output.write(buffer.array(), buffer.offset(), buffer.limit());
                } catch (IOException ex) {
                    throw Throwables.propagate(ex);
                } finally {
                    buffer.release();
                }
            }

            public void finish()
            {
                closeFile();
                if (filePath != null) {
                    try {
                        bigQueryWriter.executeLoad(project, dataset, table, filePath);

                        if (deleteFromLocalWhenJobEnd) {
                            log.info(String.format("Delete local file [%s]", filePath));
                            file.delete();
                        }
                    } catch (NoSuchAlgorithmException | TimeoutException | BigqueryWriter.JobFailedException | IOException ex) {
                        log.error(ex.getMessage());
                        throw Throwables.propagate(ex);
                    }
                }
            }

            public void close()
            {
                closeFile();
            }

            public void abort()
            {
            }

            public CommitReport commit()
            {
                CommitReport report = Exec.newCommitReport();
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

    public enum SourceFormat
    {
        CSV("CSV"),
        NEWLINE_DELIMITED_JSON("NEWLINE_DELIMITED_JSON");

        private final String string;

        private SourceFormat(String string)
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
        compute_engine("compute_engine");

        private final String string;

        private AuthMethod(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }
    }
}
