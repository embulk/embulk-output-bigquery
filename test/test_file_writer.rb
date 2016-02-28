require_relative './helper'
require 'embulk/output/bigquery/file_writer'
require 'fileutils'
require 'zlib'

class Embulk::Output::Bigquery
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
      ::Embulk::Schema.new([
        {name: 'boolean', type: :boolean},
        {name: 'long', type: :long},
        {name: 'double', type: :double},
        {name: 'string', type: :string},
        {name: 'timestamp', type: :timestamp},
        {name: 'json', type: :json},
      ])
    end

    def converters
      @converters ||= ValueConverterFactory.create_converters(default_task, schema)
    end

    sub_test_case "path" do
      def test_path
        task = default_task.merge('path_prefix' => 'foo', 'sequence_format' => '', 'file_ext' => '.1')
        file_writer = FileWriter.new(task, schema, 0, converters)
        assert_equal 'foo.1', file_writer.instance_variable_get(:@path)
      end
    end

    sub_test_case "formatter_proc" do
      def record
        [true, 1, 1.1, 'foo', Time.parse("2016-02-26 00:00:00 +09:00"), {"foo"=>"foo"}]
      end

      def test_payload_column
        task = default_task.merge('payload_column' => 'string')
        file_writer = FileWriter.new(task, schema, 0, converters)
        formatter_proc = file_writer.instance_variable_get(:@formatter_proc)
        assert_equal :to_payload, formatter_proc.name

        assert_equal %Q[foo\n], formatter_proc.call(record)
      end

      def test_csv
        task = default_task.merge('source_format' => 'CSV')
        file_writer = FileWriter.new(task, schema, 0, converters)
        formatter_proc = file_writer.instance_variable_get(:@formatter_proc)
        assert_equal :to_csv, formatter_proc.name

        expected = %Q[true,1,1.1,foo,1456412400.0,"{""foo"":""foo""}"\n]
        assert_equal expected, formatter_proc.call(record)
      end

      def test_jsonl
        task = default_task.merge('source_format' => 'NEWLINE_DELIMITED_JSON')
        file_writer = FileWriter.new(task, schema, 0, converters)
        formatter_proc = file_writer.instance_variable_get(:@formatter_proc)
        assert_equal :to_jsonl, formatter_proc.name

        expected = %Q[{"boolean":true,"long":1,"double":1.1,"string":"foo","timestamp":1456412400.0,"json":"{\\"foo\\":\\"foo\\"}"}\n]
        assert_equal expected, formatter_proc.call(record)
      end
    end

    sub_test_case "write_proc" do
      def record
        [true, 1, 1.1, 'foo', Time.parse("2016-02-26 00:00:00 +09:00"), {"foo"=>"foo"}]
      end

      def page
        [record]
      end

      def test_gzip
        task = default_task.merge('compression' => 'GZIP')
        file_writer = FileWriter.new(task, schema, 0, converters)
        write_proc = file_writer.instance_variable_get(:@write_proc)
        assert_equal :write_gzip, write_proc.name

        write_proc.call(page)
        assert_true File.exist?(file_writer.path)
        assert_nothing_raised { Zlib::GzipReader.open(file_writer.path) {|gz| } }
      end

      def test_uncompressed
        task = default_task.merge('compression' => 'NONE')
        file_writer = FileWriter.new(task, schema, 0, converters)
        write_proc = file_writer.instance_variable_get(:@write_proc)
        assert_equal :write_uncompressed, write_proc.name

        write_proc.call(page)
        assert_true File.exist?(file_writer.path)
        assert_raise { Zlib::GzipReader.open(file_writer.path) {|gz| } }
      end
    end
  end
end
