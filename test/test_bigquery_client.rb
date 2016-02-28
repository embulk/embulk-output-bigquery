require_relative './helper'
require 'embulk/output/bigquery/bigquery_client'
require 'csv'

# 1. Prepare /tmp/your-project-000.json
# 2. CONNECT=1 bunlde exec ruby test/test_bigquery_client.rb

if ENV['CONNECT']
  class Embulk::Output::Bigquery
    class TestBigqueryClient < Test::Unit::TestCase
      class << self
        def startup
          FileUtils.mkdir_p('tmp')
        end

        def shutdown
          FileUtils.rm_rf('tmp')
        end
      end

      def client(task = {})
        task = least_task.merge(task)
        BigqueryClient.new(task, schema)
      end

      def least_task
        {
          'project'          => JSON.parse(File.read('/tmp/your-project-000.json'))['project_id'],
          'dataset'          => 'your_dataset_name',
          'table'            => 'your_table_name',
          'auth_method'      => 'json_key',
          'json_keyfile'     => '/tmp/your-project-000.json',
          'retries'          => 3,
          'timeout_sec'      => 300,
          'open_timeout_sec' => 300,
          'job_status_max_polling_time' => 3600,
          'job_status_polling_interval' => 10,
          'source_format'    => 'CSV'
        }
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

      def record
        [true,1,1.1,'1',Time.parse("2016-02-26 +00:00"),'{"foo":"bar"}']
      end

      sub_test_case "client" do
        def test_json_keyfile
          assert_nothing_raised { BigqueryClient.new(least_task, schema).client }
        end

        def test_p12_keyfile
          # pending
        end
      end

      sub_test_case "create_dataset" do
        def test_create_dataset
          assert_nothing_raised { client.create_dataset }
        end
      end

      sub_test_case "create_table" do
        def test_create_table
          client.delete_table('your_table_name')
          assert_nothing_raised { client.create_table('your_table_name') }
        end

        def test_create_table_already_exists
          assert_nothing_raised { client.create_table('your_table_name') }
        end
      end

      sub_test_case "delete_table" do
        def test_delete_table
          client.create_table('your_table_name')
          assert_nothing_raised { client.delete_table('your_table_name') }
        end

        def test_delete_table_not_found
          assert_nothing_raised { client.delete_table('your_table_name') }
        end
      end

      sub_test_case "get_table" do
        def test_get_table
          client.create_table('your_table_name')
          assert_nothing_raised { client.get_table('your_table_name') }
        end

        def test_get_table_not_found
          client.delete_table('your_table_name')
          assert_raise(NotFoundError) {
            client.get_table('your_table_name')
          }
        end
      end

      sub_test_case "fields" do
        def test_fields_from_table
          client.create_table('your_table_name')
          fields = client.fields_from_table('your_table_name')
          expected = [
            {:type=>"BOOLEAN", :name=>"boolean"},
            {:type=>"INTEGER", :name=>"long"},
            {:type=>"FLOAT", :name=>"double"},
            {:type=>"STRING", :name=>"string"},
            {:type=>"TIMESTAMP", :name=>"timestamp"},
            {:type=>"STRING", :name=>"json"},
          ]
          assert_equal expected, fields
        end
      end

      sub_test_case "copy" do
        def test_create_table
          client.create_table('your_table_name')
          assert_nothing_raised { client.copy('your_table_name', 'your_table_name_old') }
        end
      end

      sub_test_case "load" do
        def test_load
          client.create_table('your_table_name')
          File.write("tmp/your_file_name.csv", record.to_csv)
          assert_nothing_raised { client.load("/tmp/your_file_name.csv", 'your_table_name') }
        end
      end
    end
  end
end
