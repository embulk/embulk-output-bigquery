require 'zlib'
require 'json'
require 'csv'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class FileWriter
        attr_reader :num_rows

        def initialize(task, schema, index, converters = nil)
          @task = task
          @schema = schema
          @index = index
          @converters = converters || ValueConverterFactory.create_converters(task, schema)

          @num_rows = 0
          if @task['progress_log_interval']
            @progress_log_interval = @task['progress_log_interval']
            @progress_log_timer = Time.now
            @previous_num_rows = 0
          end

          if @task['payload_column_index']
            @payload_column_index = @task['payload_column_index']
            @formatter_proc = self.method(:to_payload)
          else
            case @task['source_format'].downcase
            when 'csv'
              @formatter_proc = self.method(:to_csv)
            else
              @formatter_proc = self.method(:to_jsonl)
            end
          end
        end

        def io
          return @io if @io

          path = sprintf(
            "#{@task['path_prefix']}#{@task['sequence_format']}#{@task['file_ext']}",
            Process.pid, Thread.current.object_id
          )
          if File.exist?(path)
            Embulk.logger.warn { "embulk-output-bigquery: unlink already existing #{path}" }
            File.unlink(path) rescue nil
          end
          Embulk.logger.info { "embulk-output-bigquery: create #{path}" }

          @io = open(path, 'w')
        end

        def open(path, mode = 'w')
          file_io = File.open(path, mode)
          case @task['compression'].downcase
          when 'gzip'
            io = Zlib::GzipWriter.new(file_io)
          else
            io = file_io
          end
          io
        end

        def close
          io.close rescue nil
          io
        end

        def reopen
          @io = open(io.path, 'a')
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

        def num_format(number)
          number.to_s.gsub(/(\d)(?=(\d{3})+(?!\d))/, '\1,')
        end

        def add(page)
          _io = io
          # I once tried to split IO writing into another IO thread using SizedQueue
          # However, it resulted in worse performance, so I removed the codes.
          page.each do |record|
            Embulk.logger.trace { "embulk-output-bigquery: record #{record}" }
            formatted_record = @formatter_proc.call(record)
            Embulk.logger.trace { "embulk-output-bigquery: formatted_record #{formatted_record.chomp}" }
            _io.write formatted_record
            @num_rows += 1
          end
          show_progress if @task['progress_log_interval']
          @num_rows
        end

        private

        def show_progress
          now = Time.now
          if @progress_log_timer < now - @progress_log_interval
            speed = ((@num_rows - @previous_num_rows) / (now - @progress_log_timer).to_f).round(1)
            @progress_log_timer = now
            @previous_num_rows = @num_rows
            Embulk.logger.info { "embulk-output-bigquery: num_rows #{num_format(@num_rows)} (#{num_format(speed)} rows/sec)" }
          end
        end
      end
    end
  end
end
