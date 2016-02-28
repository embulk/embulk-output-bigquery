require 'zlib'
require 'json'
require 'csv'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class FileWriter
        attr_reader :path

        def initialize(task, schema, index, converters = nil)
          @task = task
          @schema = schema
          @index = index
          @converters = converters || ValueConverterFactory.create_converters(task, schema)

          @num_input_rows = 0
          @progress_log_timer = Time.now
          @previous_num_input_rows = 0

          case @task['compression'].downcase
          when 'gzip'
            @write_proc = self.method(:write_gzip)
          else
            @write_proc = self.method(:write_uncompressed)
          end

          if @task['payload_column']
            @payload_column_index = @schema.find_index {|c| c[:name] == @task['payload_column'] }
            if @payload_column_index.nil?
              raise ConfigError.new "`embulk-output-bigquery: payload_column #{@task['payload_column']}` does not exist in schema"
            end
            @formatter_proc = self.method(:to_payload)
          else
            case @task['source_format'].downcase
            when 'csv'
              @formatter_proc = self.method(:to_csv)
            else
              @formatter_proc = self.method(:to_jsonl)
            end
          end

          @path = sprintf("#{@task['path_prefix']}#{@task['sequence_format']}#{@task['file_ext']}", Process.pid, index)
          Embulk.logger.info { "embulk-output-bigquery: will create #{@path}" }
          if File.exist?(@path)
            Embulk.logger.warn { "embulk-output-bigquery: unlink already existing #{@path}" }
            File.unlink(@path) rescue nil
          end
        end

        def to_payload(record)
          "#{record[@payload_column_index]}\n"
        end

        def to_csv(record)
          record.map.with_index do |value, column_index|
            @converters[column_index].call(value)
          end.to_csv
        end

        def to_jsonl(record)
          hash = {}
          column_names = @schema.names
          record.each_with_index do |value, column_index|
            column_name = column_names[column_index]
            hash[column_name] = @converters[column_index].call(value)
          end
          "#{hash.to_json}\n"
        end

        def write_gzip(page)
          f = File.open(@path, 'a')
          # ToDo: support gzip compression level option
          Zlib::GzipWriter.wrap(f) do |io|
            write_io(io, page)
          end
        end

        def write_uncompressed(page)
          File.open(@path, 'a') do |io|
            write_io(io, page)
          end
        end

        def write_io(io, page)
          page.each do |record|
            Embulk.logger.trace { "embulk-output-bigquery: record #{record}" }
            formatted_record = @formatter_proc.call(record)
            Embulk.logger.trace { "embulk-output-bigquery: formatted_record #{formatted_record.chomp}" }
            io << formatted_record
            @num_input_rows += 1
          end
          now = Time.now
          if @progress_log_timer < now - 10 # once in 10 seconds
            speed = ((@num_input_rows - @previous_num_input_rows) / (now - @progress_log_timer).to_f).round(1)
            @progress_log_timer = now
            @previous_num_input_rows = @num_input_rows
            Embulk.logger.info { "embulk-output-bigquery: num_input_rows #{num_format(@num_input_rows)} (#{num_format(speed)} rows/sec)" }
          end
        end

        def num_format(number)
          number.to_s.gsub(/(\d)(?=(\d{3})+(?!\d))/, '\1,')
        end

        def add(page)
          # I once tried to split IO writing into another IO thread using SizedQueue
          # However, it resulted in worse performance, so I removed the codes.
          @write_proc.call(page)
        end

        def commit
          task_report = {
            'num_input_rows' => @num_input_rows,
            'path' => @path,
          }
        end
      end
    end
  end
end
