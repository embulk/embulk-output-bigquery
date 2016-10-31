require_relative './helper'
require 'embulk/output/bigquery/file_writer'
require 'fileutils'
require 'zlib'

module Embulk
  class Output::Bigquery
    class TestFileWriter < Test::Unit::TestCase
      class << self
        def startup
          FileUtils.mkdir_p('tmp')
        end

        def shutdown
          FileUtils.rm_rf('tmp')
        end
      end

      def default_task
        {
          'compression' => 'GZIP',
          'payload_column' => nil,
          'source_format' => 'CSV',
          'path_prefix' => 'tmp/path_prefix',
          'sequence_format' => '.%d.%03d',
          'file_ext' => nil,
        }
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

      def converters
        @converters ||= ValueConverterFactory.create_converters(default_task, schema)
      end

      def record
        [true, 1, 1.1, 'foo', Time.parse("2016-02-26 00:00:00 +00:00").utc, {"foo"=>"foo"}]
      end

      def page
        [record]
      end

      sub_test_case "path" do
        def test_path
          task = default_task.merge('path_prefix' => 'tmp/foo', 'sequence_format' => '', 'file_ext' => '.1')
          file_writer = FileWriter.new(task, schema, 0, converters)

          begin
            file_writer.add(page)
          ensure
            io.close rescue nil
          end
          path = file_writer.io.path
          assert_equal 'tmp/foo.1', path
        end
      end

      sub_test_case "formatter" do
        def test_payload_column_index
          task = default_task.merge('payload_column_index' => 0)
          file_writer = FileWriter.new(task, schema, 0, converters)
          formatter_proc = file_writer.instance_variable_get(:@formatter_proc)
          assert_equal :to_payload, formatter_proc.name

          assert_equal %Q[true\n], formatter_proc.call(record)
        end

        def test_csv
          task = default_task.merge('source_format' => 'CSV')
          file_writer = FileWriter.new(task, schema, 0, converters)
          formatter_proc = file_writer.instance_variable_get(:@formatter_proc)
          assert_equal :to_csv, formatter_proc.name

          expected = %Q[true,1,1.1,foo,2016-02-26 00:00:00.000000 +00:00,"{""foo"":""foo""}"\n]
          assert_equal expected, formatter_proc.call(record)
        end

        def test_jsonl
          task = default_task.merge('source_format' => 'NEWLINE_DELIMITED_JSON')
          file_writer = FileWriter.new(task, schema, 0, converters)
          formatter_proc = file_writer.instance_variable_get(:@formatter_proc)
          assert_equal :to_jsonl, formatter_proc.name

          expected = %Q[{"boolean":true,"long":1,"double":1.1,"string":"foo","timestamp":"2016-02-26 00:00:00.000000 +00:00","json":"{\\"foo\\":\\"foo\\"}"}\n]
          assert_equal expected, formatter_proc.call(record)
        end
      end

      sub_test_case "compression" do
        def test_gzip
          task = default_task.merge('compression' => 'GZIP')
          file_writer = FileWriter.new(task, schema, 0, converters)

          begin
            file_writer.add(page)
            io = file_writer.io
            assert_equal Zlib::GzipWriter, io.class
          ensure
            io.close rescue nil
          end
          path = file_writer.io.path
          assert_true File.exist?(path)
          assert_nothing_raised { Zlib::GzipReader.open(path) {|gz| } }
        end

        def test_uncompressed
          task = default_task.merge('compression' => 'NONE')
          file_writer = FileWriter.new(task, schema, 0, converters)

          begin
            file_writer.add(page)
            io = file_writer.io
            assert_equal File, io.class
          ensure
            io.close rescue nil
          end
          path = file_writer.io.path
          assert_true File.exist?(path)
          assert_raise { Zlib::GzipReader.open(path) {|gz| } }
        end
      end
    end
  end
end
