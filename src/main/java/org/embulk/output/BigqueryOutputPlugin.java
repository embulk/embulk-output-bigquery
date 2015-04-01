package org.embulk.output;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;
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
        @Config("service_account_email")
        public String getServiceAccountEmail();

        @Config("p12_keyfile_path")
        public String getP12KeyfilePath();

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
        public String getSourceFormat();

        @Config("field_delimiter")
        @ConfigDefault("\",\"")
        public String getFieldDelimiter();

        @Config("max_bad_records")
        @ConfigDefault("0")
        public int getMaxBadrecords();

        @Config("encoding")
        @ConfigDefault("\"UTF-8\"")
        public String getEncoding();

        @Config("delete_from_local_when_upload_end")
        @ConfigDefault("false")
        public boolean getDeleteFromLocalWhenUploadEnd();

        @Config("project")
        public String getProject();

        @Config("dataset")
        public String getDataset();

        @Config("table")
        public String getTable();

        @Config("auto_create_table")
        @ConfigDefault("false")
        public boolean getAutoCreateTable();

        @Config("schema_path")
        @ConfigDefault("null")
        public Optional<String> getSchemaPath();

        @Config("job_status_max_polling_time")
        @ConfigDefault("3600")
        public int getJobStatusMaxPollingTime();

        @Config("job_status_polling_interval")
        @ConfigDefault("10")
        public int getJobStatusPollingInterval();

        @Config("is_skip_job_result_check")
        @ConfigDefault("0")
        public boolean getIsSkipJobResultCheck();
    }

    private final Logger log = Exec.getLogger(BigqueryOutputPlugin.class);
    private static BigqueryWriter bigQueryWriter;

    public ConfigDiff transaction(ConfigSource config, int taskCount,
                                  FileOutputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        try {
            bigQueryWriter = new BigqueryWriter.Builder(task.getServiceAccountEmail())
                    .setP12KeyFilePath(task.getP12KeyfilePath())
                    .setApplicationName(task.getApplicationName())
                    .setProject(task.getProject())
                    .setDataset(task.getDataset())
                    .setTable(generateTableName(task.getTable()))
                    .setAutoCreateTable(task.getAutoCreateTable())
                    .setSchemaPath(task.getSchemaPath())
                    .setSourceFormat(task.getSourceFormat())
                    .setFieldDelimiter(task.getFieldDelimiter())
                    .setMaxBadrecords(task.getMaxBadrecords())
                    .setEncoding(task.getEncoding())
                    .setJobStatusMaxPollingTime(task.getJobStatusMaxPollingTime())
                    .setJobStatusPollingInterval(task.getJobStatusPollingInterval())
                    .setIsSkipJobResultCheck(task.getIsSkipJobResultCheck())
                    .build();
        } catch (FileNotFoundException ex) {
            throw new ConfigException(ex);
        } catch (IOException | GeneralSecurityException ex) {
            throw new ConfigException(ex);
        }
        // non-retryable (non-idempotent) output:
        return resume(task.dump(), taskCount, control);
    }

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
            private String fileName;
            private long fileSize;

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
                        bigQueryWriter.executeLoad(filePath);

                        if (task.getDeleteFromLocalWhenUploadEnd()) {
                            log.info(String.format("Delete local file [%s]", filePath));
                            file.delete();
                        }
                    } catch (IOException | TimeoutException | BigqueryWriter.JobFailedException ex) {
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
}