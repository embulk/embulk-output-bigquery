require 'uri'
require 'json'
require 'tempfile'
require 'fileutils'
require 'securerandom'
require_relative 'bigquery/bigquery_client'
require_relative 'bigquery/gcs_client'
require_relative 'bigquery/file_writer'
require_relative 'bigquery/value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      Plugin.register_output('bigquery', self)

      class Error < StandardError; end

      # To support configuration like below as org.embulk.spi.unit.LocalFile
      #
      # json_keyfile:
      #   content: |
      class LocalFile
        # @return JSON string
        def self.load(v)
          if v.is_a?(String) # path
            File.read(v)
          elsif v.is_a?(Hash)
            v['content']
          end
        end
      end

      def self.configure(config, schema, task_count)
        task = {
          'mode'                           => config.param('mode',                           :string,  :default => 'append'),
          'auth_method'                    => config.param('auth_method',                    :string,  :default => 'private_key'),
          'service_account_email'          => config.param('service_account_email',          :string,  :default => nil),
          'p12_keyfile'                    => config.param('p12_keyfile',                    :string,  :default => nil),
          'json_keyfile'                   => config.param('json_keyfile',                  LocalFile, :default => nil),
          'project'                        => config.param('project',                        :string,  :default => nil),
          'dataset'                        => config.param('dataset',                        :string),
          'table'                          => config.param('table',                          :string),
          'dataset_old'                    => config.param('dataset_old',                    :string,  :default => nil),
          'table_old'                      => config.param('table_old',                      :string,  :default => nil),
          'table_name_old'                 => config.param('table_name_old',                 :string,  :default => nil), # lower version compatibility
          'auto_create_dataset'            => config.param('auto_create_dataset',            :bool,    :default => false),
          'auto_create_table'              => config.param('auto_create_table',              :bool,    :default => false),
          'schema_file'                    => config.param('schema_file',                    :string,  :default => nil),
          'template_table'                 => config.param('template_table',                 :string,  :default => nil),

          'delete_from_local_when_job_end' => config.param('delete_from_local_when_job_end', :bool,    :default => true),
          'job_status_max_polling_time'    => config.param('job_status_max_polling_time',    :integer, :default => 3600),
          'job_status_polling_interval'    => config.param('job_status_polling_interval',    :integer, :default => 10),
          'is_skip_job_result_check'       => config.param('is_skip_job_result_check',       :bool,    :default => false),
          'prevent_duplicate_insert'       => config.param('prevent_duplicate_insert',       :bool,    :default => false),
          'with_rehearsal'                 => config.param('with_rehearsal',                 :bool,    :default => false),
          'rehearsal_counts'               => config.param('rehearsal_counts',               :integer, :default => 1000),
          'abort_on_error'                 => config.param('abort_on_error',                 :bool,    :default => nil),
          'progress_log_interval'          => config.param('progress_log_interval',          :float,   :default => nil),

          'column_options'                 => config.param('column_options',                 :array,   :default => []),
          'default_timezone'               => config.param('default_timezone',               :string,  :default => ValueConverterFactory::DEFAULT_TIMEZONE),
          'default_timestamp_format'       => config.param('default_timestamp_format',       :string,  :default => ValueConverterFactory::DEFAULT_TIMESTAMP_FORMAT),
          'payload_column'                 => config.param('payload_column',                 :string,  :default => nil),
          'payload_column_index'           => config.param('payload_column_index',           :integer, :default => nil),
          
          'open_timeout_sec'               => config.param('open_timeout_sec',               :integer, :default => nil),
          'timeout_sec'                    => config.param('timeout_sec',                    :integer, :default => nil), # google-api-ruby-client < v0.11.0
          'send_timeout_sec'               => config.param('send_timeout_sec',               :integer, :default => nil), # google-api-ruby-client >= v0.11.0
          'read_timeout_sec'               => config.param('read_timeout_sec',               :integer, :default => nil), # google-api-ruby-client >= v0.11.0
          'retries'                        => config.param('retries',                        :integer, :default => 5),
          'application_name'               => config.param('application_name',               :string,  :default => 'Embulk BigQuery plugin'),
          'sdk_log_level'                  => config.param('sdk_log_level',                  :string,  :default => nil),

          'path_prefix'                    => config.param('path_prefix',                    :string,  :default => nil),
          'sequence_format'                => config.param('sequence_format',                :string,  :default => '.%d.%d'),
          'file_ext'                       => config.param('file_ext',                       :string,  :default => nil),
          'skip_file_generation'           => config.param('skip_file_generation',           :bool,    :default => false),
          'compression'                    => config.param('compression',                    :string,  :default => 'NONE'),

          'gcs_bucket'                     => config.param('gcs_bucket',                     :string,  :default => nil),
          'auto_create_gcs_bucket'         => config.param('auto_create_gcs_bucket',         :bool,    :default => false),

          'source_format'                  => config.param('source_format',                  :string,  :default => 'CSV'),
          'max_bad_records'                => config.param('max_bad_records',                :integer, :default => 0),
          'field_delimiter'                => config.param('field_delimiter',                :string,  :default => ','),
          'encoding'                       => config.param('encoding',                       :string,  :default => 'UTF-8'),
          'ignore_unknown_values'          => config.param('ignore_unknown_values',          :bool,    :default => false),
          'allow_quoted_newlines'          => config.param('allow_quoted_newlines',          :bool,    :default => false),
          'time_partitioning'              => config.param('time_partitioning',              :hash,    :default => nil),
          'schema_update_options'          => config.param('schema_update_options',          :array,   :default => nil),

          # for debug
          'skip_load'                      => config.param('skip_load',                      :bool,    :default => false),
          'temp_table'                     => config.param('temp_table',                     :string,  :default => nil),
          'rehearsal_table'                => config.param('rehearsal_table',                :string,  :default => nil),
        }

        now = Time.now

        task['mode'] = task['mode'].downcase
        unless %w[append append_direct replace delete_in_advance replace_backup].include?(task['mode'])
          raise ConfigError.new "`mode` must be one of append, append_direct, replace, delete_in_advance, replace_backup"
        end

        if task['mode'] == 'replace_backup'
          task['table_old'] ||= task['table_name_old'] # for lower version compatibility
          if task['dataset_old'].nil? and task['table_old'].nil?
            raise ConfigError.new "`mode replace_backup` requires either of `dataset_old` or `table_old`"
          end
          task['dataset_old'] ||= task['dataset']
          task['table_old']   ||= task['table']
        end

        if task['table_old']
          task['table_old'] = now.strftime(task['table_old'])
        end
        if task['table']
          task['table'] = now.strftime(task['table'])
        end

        task['auth_method'] = task['auth_method'].downcase
        unless %w[private_key json_key compute_engine application_default].include?(task['auth_method'])
          raise ConfigError.new "`auth_method` must be one of private_key, json_key, compute_engine, application_default"
        end
        if task['auth_method'] == 'private_key' and task['p12_keyfile'].nil?
          raise ConfigError.new "`p12_keyfile` is required for auth_method private_key"
        end
        if task['auth_method'] == 'json_key' and task['json_keyfile'].nil?
          raise ConfigError.new "`json_keyfile` is required for auth_method json_key"
        end

        jsonkey_params = nil
        if task['json_keyfile']
          begin
            jsonkey_params = JSON.parse(task['json_keyfile'])
          rescue => e
            raise ConfigError.new "json_keyfile is not a JSON file"
          end
        end

        if jsonkey_params
          task['project'] ||= jsonkey_params['project_id']
        end
        if task['project'].nil?
          raise ConfigError.new "Required field \"project\" is not set"
        end

        if (task['payload_column'] or task['payload_column_index']) and task['auto_create_table']
          if task['schema_file'].nil? and task['template_table'].nil?
            raise ConfigError.new "Cannot guess table schema from Embulk schema with `payload_column` or `payload_column_index`. Either of `schema_file` or `template_table` is required for auto_create_table true"
          end
        end

        if task['payload_column_index']
          if task['payload_column_index'] < 0 || schema.size <= task['payload_column_index']
            raise ConfigError.new "payload_column_index #{task['payload_column_index']} is out of schema size"
          end
        elsif task['payload_column']
          task['payload_column_index'] = schema.find_index {|c| c[:name] == task['payload_column'] }
          if task['payload_column_index'].nil?
            raise ConfigError.new "payload_column #{task['payload_column']} does not exist in schema"
          end
        end

        if task['schema_file']
          unless File.exist?(task['schema_file'])
            raise ConfigError.new "schema_file #{task['schema_file']} is not found"
          end
          begin
            JSON.parse(File.read(task['schema_file']))
          rescue => e
            raise ConfigError.new "schema_file #{task['schema_file']} is not a JSON file"
          end
        end

        if task['path_prefix'].nil?
          task['path_prefix'] = Tempfile.create('embulk_output_bigquery_') {|fp| fp.path }
        end

        task['source_format'] = task['source_format'].upcase
        if task['source_format'] == 'JSONL'
          task['source_format'] = 'NEWLINE_DELIMITED_JSON'
        end
        unless %w[CSV NEWLINE_DELIMITED_JSON].include?(task['source_format'])
          raise ConfigError.new "`source_format` must be CSV or NEWLINE_DELIMITED_JSON (JSONL)"
        end

        task['compression'] = task['compression'].upcase
        unless %w[GZIP NONE].include?(task['compression'])
          raise ConfigError.new "`compression` must be GZIP or NONE"
        end

        if task['file_ext'].nil?
          case task['source_format']
          when 'CSV'
            file_ext = '.csv'
          else # newline_delimited_json
            file_ext = '.jsonl'
          end
          case task['compression']
          when 'GZIP'
            file_ext << '.gz'
          end
          task['file_ext'] = file_ext
        end

        unique_name = SecureRandom.uuid.gsub('-', '_')

        if %w[replace replace_backup append].include?(task['mode'])
          task['temp_table'] ||= "LOAD_TEMP_#{unique_name}_#{task['table']}"
        else
          task['temp_table'] = nil
        end

        if task['with_rehearsal']
          task['rehearsal_table'] ||= "LOAD_REHEARSAL_#{unique_name}_#{task['table']}"
        end

        if task['sdk_log_level']
          Google::Apis.logger.level = eval("::Logger::#{task['sdk_log_level'].upcase}")
        end

        if task['abort_on_error'].nil?
          task['abort_on_error'] = (task['max_bad_records'] == 0)
        end

        if task['time_partitioning']
          unless task['time_partitioning']['type']
            raise ConfigError.new "`time_partitioning` must have `type` key"
          end
        elsif Helper.has_partition_decorator?(task['table'])
          task['time_partitioning'] = {'type' => 'DAY'}
        end

        if task['schema_update_options']
          task['schema_update_options'].each do |schema_update_option|
            unless %w[ALLOW_FIELD_ADDITION ALLOW_FIELD_RELAXATION].include?(schema_update_option)
              raise ConfigError.new "`schema_update_options` must contain either of ALLOW_FIELD_ADDITION or ALLOW_FIELD_RELAXATION or both"
            end
          end
        end

        task
      end

      def self.bigquery
        @bigquery
      end

      def self.converters
        @converters
      end

      def self.rehearsal_thread
        @rehearsal_thread
      end

      def self.rehearsal_thread=(rehearsal_thread)
        @rehearsal_thread = rehearsal_thread
      end

      def self.transaction_report(task, responses)
        num_input_rows = file_writers.empty? ? 0 : file_writers.map(&:num_rows).inject(:+)
        return {'num_input_rows' => num_input_rows} if task['is_skip_job_result_check']

        num_response_rows = responses.inject(0) do |sum, response|
          sum + (response ? response.statistics.load.output_rows.to_i : 0)
        end
        if task['temp_table']
          num_output_rows = bigquery.get_table(task['temp_table']).num_rows.to_i
        else
          num_output_rows = num_response_rows
        end
        num_rejected_rows = num_input_rows - num_output_rows
        transaction_report = {
          'num_input_rows' => num_input_rows,
          'num_response_rows' => num_response_rows,
          'num_output_rows' => num_output_rows,
          'num_rejected_rows' => num_rejected_rows,
        }
      end

      def self.auto_create(task, bigquery)
        if task['auto_create_dataset']
          bigquery.create_dataset(task['dataset'])
        else
          bigquery.get_dataset(task['dataset']) # raises NotFoundError
        end

        if task['mode'] == 'replace_backup' and task['dataset_old'] != task['dataset']
          if task['auto_create_dataset']
            bigquery.create_dataset(task['dataset_old'], reference: task['dataset'])
          else
            bigquery.get_dataset(task['dataset_old']) # raises NotFoundError
          end
        end

        case task['mode']
        when 'delete_in_advance'
          if task['time_partitioning']
            bigquery.delete_partition(task['table'])
          else
            bigquery.delete_table(task['table'])
          end
          bigquery.create_table(task['table'])
        when 'replace', 'replace_backup', 'append'
          bigquery.create_table(task['temp_table'])
          if task['time_partitioning']
            if task['auto_create_table']
              bigquery.create_table(task['table'])
            else
              bigquery.get_table(task['table']) # raises NotFoundError
            end
          end
        else # append_direct
          if task['auto_create_table']
            bigquery.create_table(task['table'])
          else
            bigquery.get_table(task['table']) # raises NotFoundError
          end
        end

        if task['mode'] == 'replace_backup'
          if task['time_partitioning'] and Helper.has_partition_decorator?(task['table_old'])
            if task['auto_create_table']
              bigquery.create_table(task['table_old'], dataset: task['dataset_old'])
            else
              bigquery.get_table(task['table_old'], dataset: task['dataset_old']) # raises NotFoundError
            end
          end
        end
      end

      def self.transaction(config, schema, task_count, &control)
        task = self.configure(config, schema, task_count)

        @task = task
        @schema = schema
        @bigquery = BigqueryClient.new(task, schema)
        @converters = ValueConverterFactory.create_converters(task, schema)

        self.auto_create(@task, @bigquery)

        begin
          paths = []
          if task['skip_file_generation']
            yield(task) # does nothing, but seems it has to be called
            path_pattern = "#{task['path_prefix']}*#{task['file_ext']}"
            Embulk.logger.info { "embulk-output-bigquery: Skip file generation. Get paths from `#{path_pattern}`" }
            paths = Dir.glob(path_pattern)
          else
            task_reports = yield(task) # generates local files

            ios = file_writers.map(&:io)
            paths = ios.map(&:path)
            ios.each do |io|
              Embulk.logger.debug { "close #{io.path}" }
              io.close rescue nil
            end
          end

          if rehearsal_thread
            rehearsal_thread.join
          end

          if task['skip_load'] # only for debug
            Embulk.logger.info { "embulk-output-bigquery: Skip load" }
          else
            if !paths.empty?
              target_table = task['temp_table'] ? task['temp_table'] : task['table']
              if bucket = task['gcs_bucket']
                gcs = GcsClient.new(task)
                gcs.insert_bucket(bucket) if task['auto_create_gcs_bucket']
                objects = paths.size.times.map { SecureRandom.uuid.to_s }
                gcs.insert_objects(paths, objects: objects, bucket: bucket)
                object_uris = objects.map {|object| URI.join("gs://#{bucket}", object).to_s }
                responses = bigquery.load_from_gcs(object_uris, target_table)
                objects.each {|object| gcs.delete_object(object, bucket: bucket) }
              else
                responses = bigquery.load_in_parallel(paths, target_table)
              end
            else
              responses = []
            end
            transaction_report = self.transaction_report(task, responses)
            Embulk.logger.info { "embulk-output-bigquery: transaction_report: #{transaction_report.to_json}" }

            if task['abort_on_error'] && !task['is_skip_job_result_check']
              if transaction_report['num_input_rows'] != transaction_report['num_output_rows']
                raise Error, "ABORT: `num_input_rows (#{transaction_report['num_input_rows']})` and " \
                  "`num_output_rows (#{transaction_report['num_output_rows']})` does not match"
              end
            end

            if task['mode'] == 'replace_backup'
              begin
                bigquery.get_table(task['table'])
                bigquery.copy(task['table'], task['table_old'], task['dataset_old'])
              rescue NotFoundError
              end
            end

            if task['temp_table']
              if task['mode'] == 'append'
                bigquery.copy(task['temp_table'], task['table'], write_disposition: 'WRITE_APPEND')
              else # replace or replace_backup
                bigquery.copy(task['temp_table'], task['table'], write_disposition: 'WRITE_TRUNCATE')
              end
            end
          end
        ensure
          begin
            if task['temp_table'] # append or replace or replace_backup
              bigquery.delete_table(task['temp_table'])
            end
          ensure
            if task['delete_from_local_when_job_end']
              paths.each do |path|
                Embulk.logger.info { "embulk-output-bigquery: delete #{path}" }
                File.unlink(path) rescue nil
              end
            else
              paths.each do |path|
                if File.exist?(path)
                  Embulk.logger.info { "embulk-output-bigquery: keep #{path}" }
                end
              end
            end
          end
        end

        # this is for -c next_config option, add some paramters for next execution if wants
        next_config_diff = {}
        return next_config_diff
      end

      @file_writers_mutex = Mutex.new
      @file_writers = Array.new

      def self.reset_file_writers
        @file_writers = Array.new
      end

      def self.file_writers
        @file_writers
      end

      def self.add_file_writer(file_writer)
        @file_writers_mutex.synchronize do
          @file_writers << file_writer
        end
      end

      FILE_WRITER_KEY = :embulk_output_bigquery_file_writer

      # Create one FileWriter object for one output thread, that is, share among tasks.
      # Close theses shared objects in transaction.
      # This is mainly to suppress (or control by -X max_threads) number of files, which
      # equals to number of concurrency to load in parallel, when number of input tasks is many
      #
      # #file_writer must be called at only #add because threads in other methods
      # are different (called from non-output threads). Note also that #add method
      # of the same task instance would be called in different output threads
      def file_writer
        return Thread.current[FILE_WRITER_KEY] if Thread.current[FILE_WRITER_KEY]
        file_writer = FileWriter.new(@task, @schema, @index, self.class.converters)
        self.class.add_file_writer(file_writer)
        Thread.current[FILE_WRITER_KEY] = file_writer
      end

      # instance is created on each task
      def initialize(task, schema, index)
        super

        if task['with_rehearsal'] and @index == 0
          @rehearsaled = false
        end
      end

      # called for each page in each task
      def close
      end

      # called for each page in each task
      def add(page)
        return if task['skip_file_generation']
        num_rows = file_writer.add(page)

        if task['with_rehearsal'] and @index == 0 and !@rehearsaled
          if num_rows >= task['rehearsal_counts']
            load_rehearsal
            @rehearsaled = true
          end
        end
      end

      def load_rehearsal
        bigquery = self.class.bigquery
        Embulk.logger.info { "embulk-output-bigquery: Rehearsal started" }

        io = file_writer.close # need to close once for gzip
        rehearsal_path = "#{io.path}.rehearsal"
        Embulk.logger.debug { "embulk_output_bigquery: cp #{io.path} #{rehearsal_path}" }
        FileUtils.cp(io.path, rehearsal_path)
        file_writer.reopen

        self.class.rehearsal_thread = Thread.new do
          begin
            bigquery.create_table(task['rehearsal_table'])
            response = bigquery.load(rehearsal_path, task['rehearsal_table'])
            num_output_rows = response ? response.statistics.load.output_rows.to_i : 0
            Embulk.logger.info { "embulk-output-bigquery: Loaded rehearsal #{num_output_rows}" }
          ensure
            Embulk.logger.debug { "embulk_output_bigquery: delete #{rehearsal_path}" }
            File.unlink(rehearsal_path) rescue nil
            bigquery.delete_table(task['rehearsal_table'])
          end
        end
      end

      def finish
      end

      def abort
      end

      # called after processing all pages in each task, returns a task_report
      def commit
        {}
      end
    end
  end
end
