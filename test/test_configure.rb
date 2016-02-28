require_relative './helper'
require 'embulk/output/bigquery'

Bigquery = Embulk::Output::Bigquery unless defined?(Bigquery)

class Embulk::Output::Bigquery
  class TestConfigure < Test::Unit::TestCase
    class << self
      def startup
        FileUtils.mkdir_p('tmp')
      end

      def shutdown
        FileUtils.rm_rf('tmp')
      end
    end

    def least_config
      ::Embulk::DataSource.new({
        'project' => 'your_project_name',
        'dataset' => 'your_dataset_name',
        'table'   => 'your_table_name',
        'p12_keyfile' => __FILE__, # fake
      })
    end

    def schema
      ::Embulk::Schema.new([
        {name: 'boolean', type: :boolean},
        {name: 'long', type: :long},
        {name: 'double', type: :double},
        {name: 'string', type: :string},
        {name: 'timestamp', type: :timestamp},
        {name: 'json', type: :json},
      ])
    end

    def processor_count
      1
    end

    def test_configure_default
      task = Bigquery.configure(least_config, schema, processor_count)
      assert_equal "append", task['mode']
      assert_equal "private_key", task['auth_method']
      assert_equal nil, task['service_account_email']
      assert_equal __FILE__, task['p12_keyfile']
      assert_equal nil, task['json_keyfile']
      assert_equal "your_project_name", task['project']
      assert_equal "your_dataset_name", task['dataset']
      assert_equal "your_table_name", task['table']
      assert_equal nil, task['dataset_old']
      assert_equal nil, task['table_old']
      assert_equal nil, task['table_name_old']
      assert_equal false, task['auto_create_table']
      assert_equal nil, task['schema_file']
      assert_equal nil, task['template_table']
      assert_equal true, task['delete_from_local_when_job_end']
      assert_equal 3600, task['job_status_max_polling_time']
      assert_equal 10, task['job_status_polling_interval']
      assert_equal false, task['is_skip_job_result_check']
      assert_equal false, task['prevent_duplicate_insert']
      assert_equal false, task['with_rehearsal']
      assert_equal 1000, task['rehearsal_counts']
      assert_equal [], task['column_options']
      assert_equal "UTC", task['default_timezone']
      assert_equal "%Y-%m-%d %H:%M:%S.%6N", task['default_timestamp_format']
      assert_equal nil, task['payload_column']
      assert_equal 300, task['timeout_sec']
      assert_equal 300, task['open_timeout_sec']
      assert_equal 5, task['retries']
      assert_equal "Embulk BigQuery plugin", task['application_name']
      assert_equal nil, task['max_load_parallels']
      # assert_equal "/tmp/embulk_output_bigquery_20160228-27184-pubcn0", task['path_prefix']
      assert_equal ".%d.%03d", task['sequence_format']
      assert_equal ".csv", task['file_ext']
      assert_equal false, task['skip_file_generation']
      assert_equal "NONE", task['compression']
      assert_equal "CSV", task['source_format']
      assert_equal 0, task['max_bad_records']
      assert_equal ",", task['field_delimiter']
      assert_equal "UTF-8", task['encoding']
      assert_equal false, task['ignore_unknown_values']
      assert_equal false, task['allow_quoted_newlines']
      assert_equal false, task['skip_load']
    end

    def test_mode
      config = least_config.merge('mode' => 'foobar')
      assert_raise { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('mode' => 'append')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('mode' => 'replace')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('mode' => 'delete_in_advance')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('mode' => 'replace_backup')
      assert_raise { Bigquery.configure(config, schema, processor_count) }
      config = least_config.merge('mode' => 'replace_backup', 'table_old' => 'backup')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }
      config = least_config.merge('mode' => 'replace_backup', 'dataset_old' => 'backup')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }
    end

    def test_auth_method
      config = least_config.merge('auth_method' => 'foobar')
      assert_raise { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('auth_method' => 'private_key').tap {|h| h.delete('p12_keyfile') }
      assert_raise { Bigquery.configure(config, schema, processor_count) }
      config = least_config.merge('auth_method' => 'private_key', 'p12_keyfile' => 'dummy')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('auth_method' => 'json_key').tap {|h| h.delete('json_keyfile') }
      assert_raise { Bigquery.configure(config, schema, processor_count) }
      config = least_config.merge('auth_method' => 'json_key', 'json_keyfile' => "#{EXAMPLE_ROOT}/json_key.json")
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('auth_method' => 'compute_engine')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }
    end

    def test_json_keyfile
      json_keyfile = "#{EXAMPLE_ROOT}/json_key.json"
      config = least_config.merge('auth_method' => 'json_key', 'json_keyfile' => json_keyfile).tap {|h| h.delete('project') }
      task = Bigquery.configure(config, schema, processor_count)
      assert_not_equal nil, task['project'] # project is obtained from json_keyfile if available

      config = least_config.merge('auth_method' => 'json_key', 'json_keyfile' => { 'content' => File.read(json_keyfile) }).tap {|h| h.delete('project') }
      task = Bigquery.configure(config, schema, processor_count)
      assert_not_equal nil, task['project'] # project is obtained from json_keyfile if available

      config = least_config.merge('auth_method' => 'json_key', 'json_keyfile' => { 'content' => 'not a json' })
      assert_raise { Bigquery.configure(config, schema, processor_count) }
    end

    def test_auto_create_table_with_payload_column
      config = least_config.merge('auto_create_table' => true, 'payload_column' => 'foo')
      assert_raise { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('auto_create_table' => true, 'payload_column' => 'foo', 'schema_file' => "#{EXAMPLE_ROOT}/schema.json")
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('auto_create_table' => true, 'payload_column' => 'foo', 'template_table' => 'foo')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }
    end

    def test_schema_file
      config = least_config.merge('schema_file' => "#{EXAMPLE_ROOT}/schema.json")
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('schema_file' => "not_found.json")
      assert_raise { Bigquery.configure(config, schema, processor_count) }

      File.write("tmp/bad_schema.json", "not_a_json")
      config = least_config.merge('schema_file' => "tmp/bad_schema.json")
      assert_raise { Bigquery.configure(config, schema, processor_count) }
    end

    def test_source_format
      config = least_config.merge('source_format' => 'csv')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('source_format' => 'jsonl')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('source_format' => 'newline_delimited_json')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('source_format' => 'foobar')
      assert_raise { Bigquery.configure(config, schema, processor_count) }
    end

    def test_compression
      config = least_config.merge('compression' => 'gzip')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('compression' => 'none')
      assert_nothing_raised { Bigquery.configure(config, schema, processor_count) }

      config = least_config.merge('compression' => 'foobar')
      assert_raise { Bigquery.configure(config, schema, processor_count) }
    end

    def test_file_ext
      config = least_config.merge('source_format' => 'csv', 'compression' => 'gzip')
      task = Bigquery.configure(config, schema, processor_count)
      assert_equal '.csv.gz', task['file_ext']

      config = least_config.merge('source_format' => 'NEWLINE_DELIMITED_JSON', 'compression' => 'gzip')
      task = Bigquery.configure(config, schema, processor_count)
      assert_equal '.jsonl.gz', task['file_ext']

      config = least_config.merge('source_format' => 'csv', 'compression' => 'none')
      task = Bigquery.configure(config, schema, processor_count)
      assert_equal '.csv', task['file_ext']

      config = least_config.merge('source_format' => 'NEWLINE_DELIMITED_JSON', 'compression' => 'none')
      task = Bigquery.configure(config, schema, processor_count)
      assert_equal '.jsonl', task['file_ext']

      config = least_config.merge('file_ext' => '.foo')
      task = Bigquery.configure(config, schema, processor_count)
      assert_equal '.foo', task['file_ext']
    end
  end
end
