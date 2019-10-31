require_relative './helper'
require 'embulk/output/bigquery/helper'

module Embulk
  class Output::Bigquery
    class TestHelper < Test::Unit::TestCase
      class << self
        def startup
          FileUtils.mkdir_p('tmp')
        end

        def shutdown
          FileUtils.rm_rf('tmp')
        end
      end

      def has_partition_decorator?
        assert_true Helper.has_partition_decorator?('table$20160929')
        assert_false Helper.has_partition_decorator?('table')
      end

      def chomp_partition_decorator
        assert_equal 'table', Helper.chomp_partition_decorator?('table$20160929')
        assert_equal 'table', Helper.chomp_partition_decorator?('table')
      end

      def bq_type_from_embulk_type
        assert_equal 'BOOLEAN',   Helper.bq_type_from_embulk_type(:boolean)
        assert_equal 'STRING',    Helper.bq_type_from_embulk_type(:string)
        assert_equal 'FLOAT',     Helper.bq_type_from_embulk_type(:double)
        assert_equal 'STRING',    Helper.bq_type_from_embulk_type(:string)
        assert_equal 'TIMESTAMP', Helper.bq_type_from_embulk_type(:timestamp)
        assert_equal 'STRING',    Helper.bq_type_from_embulk_type(:json)
      end

      sub_test_case "fields_from_embulk_schema" do
        def test_fields_from_embulk_schema_without_column_options
          schema = Schema.new([
            Column.new({index: 0, name: 'boolean', type: :boolean}),
            Column.new({index: 1, name: 'long', type: :long}),
            Column.new({index: 2, name: 'double', type: :double}),
            Column.new({index: 3, name: 'string', type: :string}),
            Column.new({index: 4, name: 'timestamp', type: :timestamp}),
            Column.new({index: 5, name: 'json', type: :json}),
          ])
          expected = [
            {name: 'boolean',   type: 'BOOLEAN'},
            {name: 'long',      type: 'INTEGER'},
            {name: 'double',    type: 'FLOAT'},
            {name: 'string',    type: 'STRING'},
            {name: 'timestamp', type: 'TIMESTAMP'},
            {name: 'json',      type: 'STRING'},
          ]
          fields = Helper.fields_from_embulk_schema({}, schema)
          assert_equal expected, fields
        end

        def test_fields_from_embulk_schema_with_column_options
          schema = Schema.new([
            Column.new({index: 0, name: 'boolean', type: :boolean}),
            Column.new({index: 1, name: 'long', type: :long}),
            Column.new({index: 2, name: 'double', type: :double}),
            Column.new({index: 3, name: 'string', type: :string}),
            Column.new({index: 4, name: 'timestamp', type: :timestamp}),
            Column.new({index: 5, name: 'date', type: :timestamp}),
            Column.new({index: 6, name: 'datetime', type: :timestamp}),
            Column.new({index: 7, name: 'json', type: :json}),
          ])
          task = {
            'column_options' => [
              {'name' => 'boolean',   'type' => 'STRING', 'mode' => 'REQUIRED'},
              {'name' => 'long',      'type' => 'STRING'},
              {'name' => 'double',    'type' => 'STRING'},
              {'name' => 'string',    'type' => 'INTEGER'},
              {'name' => 'timestamp', 'type' => 'INTEGER'},
              {'name' => 'date',      'type' => 'DATE'},
              {'name' => 'datetime',  'type' => 'DATETIME'},
              {'name' => 'json',      'type' => 'RECORD', 'fields' => [
                { 'name' => 'key1',   'type' => 'STRING' },
              ]},
            ],
          }
          expected = [
            {name: 'boolean',   type: 'STRING', mode: 'REQUIRED'},
            {name: 'long',      type: 'STRING'},
            {name: 'double',    type: 'STRING'},
            {name: 'string',    type: 'INTEGER'},
            {name: 'timestamp', type: 'INTEGER'},
            {name: 'date',      type: 'DATE'},
            {name: 'datetime',  type: 'DATETIME'},
            {name: 'json',      type: 'RECORD', fields: [
              {name: 'key1',    type: 'STRING'},
            ]},
          ]
          fields = Helper.fields_from_embulk_schema(task, schema)
          assert_equal expected, fields
        end
      end

      def test_create_load_job_id
        task = {
          'dataset' => 'your_dataset_name',
          'location' => 'asia-northeast1',
          'table' => 'your_table_name',
          'source_format' => 'CSV',
          'max_bad_records' => nil,
          'field_delimiter' => ',',
          'encoding' => 'UTF-8',
          'ignore_unknown_values' => nil,
          'allow_quoted_newlines' => nil,
        }
        fields = {
          name: 'a', type: 'STRING',
        }
        File.write("tmp/your_file_name", "foobarbaz")
        job_id = Helper.create_load_job_id(task, 'tmp/your_file_name', fields)
        assert job_id.is_a?(String)
        assert_equal 'embulk_load_job_2abaf528b69987db0224e52bbd1f0eec', job_id
      end
    end
  end
end
