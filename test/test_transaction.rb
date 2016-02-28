require_relative './helper'
require 'embulk/output/bigquery'

Bigquery = Embulk::Output::Bigquery unless defined?(Bigquery)

class Embulk::Output::Bigquery
  class TestTransaction < Test::Unit::TestCase
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

    def control
      Proc.new {|task| task_reports = [] }
    end

    def test_append
      config = least_config.merge('mode' => 'append')
      any_instance_of(BigqueryClient) do |obj|
        mock(obj).create_dataset(config['dataset'])
        mock(obj).get_table(config['table'])
        mock(obj).load_in_parallel(anything, config['table']) { [] }
      end
      Bigquery.transaction(config, schema, processor_count, &control)
    end

    def test_delete_in_advance
      config = least_config.merge('mode' => 'delete_in_advance')
      any_instance_of(BigqueryClient) do |obj|
        mock(obj).create_dataset(config['dataset'])
        mock(obj).delete_table(config['table'])
        mock(obj).create_table(config['table'])
        mock(obj).load_in_parallel(anything, config['table']) { [] }
      end
      Bigquery.transaction(config, schema, processor_count, &control)
    end

    def test_replace
      config = least_config.merge('mode' => 'replace', 'temp_table' => 'temp_table')
      any_instance_of(BigqueryClient) do |obj|
        mock(obj).create_dataset(config['dataset'])
        mock(obj).create_table(config['temp_table'])
        mock(obj).load_in_parallel(anything, config['temp_table']) { [] }
        mock(obj).copy(config['temp_table'], config['table'])
        mock(obj).delete_table(config['temp_table'])
      end
      Bigquery.transaction(config, schema, processor_count, &control)
    end

    def test_replace_backup
      config = least_config.merge('mode' => 'replace_backup', 'dataset_old' => 'dataset_old', 'table_old' => 'table_old', 'temp_table' => 'temp_table')
      any_instance_of(BigqueryClient) do |obj|
        mock(obj).create_dataset(config['dataset'])
        mock(obj).create_table(config['temp_table'])
        mock(obj).load_in_parallel(anything, config['temp_table']) { [] }

        mock(obj).create_dataset(config['dataset_old'])
        mock(obj).copy(config['table'], config['table_old'], config['dataset_old'])

        mock(obj).copy(config['temp_table'], config['table'])
        mock(obj).delete_table(config['temp_table'])
      end
      Bigquery.transaction(config, schema, processor_count, &control)
    end
  end
end
