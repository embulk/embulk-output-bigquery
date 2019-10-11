require_relative './helper'
require 'embulk/output/bigquery'

Bigquery = Embulk::Output::Bigquery unless defined?(Bigquery)

module Embulk
  class Output::Bigquery
    class TestTransaction < Test::Unit::TestCase
      def least_config
        DataSource.new({
          'project'      => 'your_project_name',
          'dataset'      => 'your_dataset_name',
          'table'        => 'your_table_name',
          'temp_table'   => 'temp_table', # randomly created is not good for our test
          'path_prefix'  => 'tmp/', # randomly created is not good for our test
        })
      end

      def schema
        Schema.new([
          Column.new({index: 0, name: 'boolean', type: :boolean}),
          Column.new({index: 1, name: 'long', type: :long}),
          Column.new({index: 2, name: 'double', type: :double}),
          Column.new({index: 3, name: 'string', type: :string}),
          Column.new({index: 4, name: 'timestamp', type: :timestamp}),
          Column.new({index: 5, name: 'json', type: :json}),
        ])
      end

      def processor_count
        1
      end

      def control
        Proc.new {|task| task_reports = [] }
      end

      def setup
        stub(Bigquery).transaction_report { {'num_input_rows' => 1, 'num_output_rows' => 1, 'num_rejected_rows' => 0} }
      end

      sub_test_case "append_direct" do
        def test_append_direc_without_auto_create
          config = least_config.merge('mode' => 'append_direct', 'auto_create_dataset' => false, 'auto_create_table' => false)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).get_table(config['table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_append_direct_with_auto_create
          config = least_config.merge('mode' => 'append_direct', 'auto_create_dataset' => true, 'auto_create_table' => true)
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).create_dataset(config['dataset'])
            mock(obj).create_table_if_not_exists(config['table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_append_direct_with_partition_without_auto_create
          config = least_config.merge('mode' => 'append_direct', 'table' => 'table$20160929', 'auto_create_dataset' => false, 'auto_create_table' => false)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).get_table(config['table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_append_direct_with_partition_with_auto_create
          config = least_config.merge('mode' => 'append_direct', 'table' => 'table$20160929', 'auto_create_dataset' => true, 'auto_create_table' => true)
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).create_dataset(config['dataset'])
            mock(obj).create_table_if_not_exists(config['table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end
      end

      sub_test_case "delete_in_advance" do
        def test_delete_in_advance
          config = least_config.merge('mode' => 'delete_in_advance')
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).delete_table_or_partition(config['table'])
            mock(obj).create_table_if_not_exists(config['table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_delete_in_advance_with_partitioning
          config = least_config.merge('mode' => 'delete_in_advance', 'table' => 'table$20160929', 'auto_create_table' => true)
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).delete_table_or_partition(config['table'])
            mock(obj).create_table_if_not_exists(config['table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end
      end

      sub_test_case "replace" do
        def test_replace
          config = least_config.merge('mode' => 'replace')
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).create_table_if_not_exists(config['temp_table'])
            mock(obj).create_table_if_not_exists(config['table'])
            mock(obj).copy(config['temp_table'], config['table'], write_disposition: 'WRITE_TRUNCATE')
            mock(obj).delete_table(config['temp_table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_replace_with_partitioning
          config = least_config.merge('mode' => 'replace', 'table' => 'table$20160929')
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).create_table_if_not_exists(config['temp_table'])
            mock(obj).create_table_if_not_exists(config['table'])
            mock(obj).copy(config['temp_table'], config['table'], write_disposition: 'WRITE_TRUNCATE')
            mock(obj).delete_table(config['temp_table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end
      end

      sub_test_case "replace_backup" do
        def test_replace_backup
          config = least_config.merge('mode' => 'replace_backup', 'dataset_old' => 'dataset_old', 'table_old' => 'table_old', 'temp_table' => 'temp_table')
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).get_dataset(config['dataset_old'])
            mock(obj).create_table_if_not_exists(config['temp_table'])
            mock(obj).create_table_if_not_exists(config['table'])
            mock(obj).create_table_if_not_exists(config['table_old'], dataset: config['dataset_old'])

            mock(obj).get_table_or_partition(config['table'])
            mock(obj).copy(config['table'], config['table_old'], config['dataset_old'])

            mock(obj).copy(config['temp_table'], config['table'], write_disposition: 'WRITE_TRUNCATE')
            mock(obj).delete_table(config['temp_table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_replace_backup_auto_create_dataset
          config = least_config.merge('mode' => 'replace_backup', 'dataset_old' => 'dataset_old', 'table_old' => 'table_old', 'temp_table' => 'temp_table', 'auto_create_dataset' => true)
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).create_dataset(config['dataset'])
            mock(obj).create_dataset(config['dataset_old'], reference: config['dataset'])
            mock(obj).create_table_if_not_exists(config['table'])
            mock(obj).create_table_if_not_exists(config['temp_table'])
            mock(obj).create_table_if_not_exists(config['table_old'], dataset: config['dataset_old'])

            mock(obj).get_table_or_partition(config['table'])
            mock(obj).copy(config['table'], config['table_old'], config['dataset_old'])

            mock(obj).copy(config['temp_table'], config['table'], write_disposition: 'WRITE_TRUNCATE')
            mock(obj).delete_table(config['temp_table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_replace_backup_with_partitioning
          config = least_config.merge('mode' => 'replace_backup', 'table' => 'table$20160929', 'dataset_old' => 'dataset_old', 'table_old' => 'table_old$20160929', 'temp_table' => 'temp_table', 'auto_create_table' => true)
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).get_dataset(config['dataset_old'])
            mock(obj).create_table_if_not_exists(config['temp_table'])
            mock(obj).create_table_if_not_exists(config['table'])
            mock(obj).create_table_if_not_exists(config['table_old'], dataset: config['dataset_old'])

            mock(obj).get_table_or_partition(config['table'])
            mock(obj).copy(config['table'], config['table_old'], config['dataset_old'])

            mock(obj).copy(config['temp_table'], config['table'], write_disposition: 'WRITE_TRUNCATE')
            mock(obj).delete_table(config['temp_table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end
      end

      sub_test_case "append" do
        def test_append
          config = least_config.merge('mode' => 'append')
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).create_table_if_not_exists(config['temp_table'])
            mock(obj).create_table_if_not_exists(config['table'])
            mock(obj).copy(config['temp_table'], config['table'], write_disposition: 'WRITE_APPEND')
            mock(obj).delete_table(config['temp_table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end

        def test_append_with_partitioning
          config = least_config.merge('mode' => 'append', 'table' => 'table$20160929', 'auto_create_table' => true)
          task = Bigquery.configure(config, schema, processor_count)
          any_instance_of(BigqueryClient) do |obj|
            mock(obj).get_dataset(config['dataset'])
            mock(obj).create_table_if_not_exists(config['temp_table'])
            mock(obj).create_table_if_not_exists(config['table'])
            mock(obj).copy(config['temp_table'], config['table'], write_disposition: 'WRITE_APPEND')
            mock(obj).delete_table(config['temp_table'])
          end
          Bigquery.transaction(config, schema, processor_count, &control)
        end
      end
    end
  end
end
