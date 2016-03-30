require 'zlib'
require 'json'
require 'csv'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class FileWriter
        def initialize(task, schema, index, converters = nil)
          @task = task
          @schema = schema
          @index = index
          @converters = converters || ValueConverterFactory.create_converters(task, schema)

          @num_input_rows = 0
          @progress_log_timer = Time.now
          @previous_num_input_rows = 0

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

        @mutex = Mutex.new
        @ios = Hash.new

        def self.mutex
          @mutex
        end

        def self.reset_ios
          @ios = Hash.new
        end

        def self.ios
          @ios
        end

        def self.paths
          @ios.keys
        end

        THREAD_LOCAL_IO_KEY = :embulk_output_bigquery_file_writer_io

        # Create one io object for one output thread, that is, share among tasks
        # Close theses shared io objects in transaction
        #
        # Thread IO must be created at #add because threads in #initialize or #commit
        # are different (called from non-output threads). Note also that #add of the
        # same instance would be called in different output threads
        def thread_io
          return Thread.current[THREAD_LOCAL_IO_KEY] if Thread.current[THREAD_LOCAL_IO_KEY]

          path = sprintf(
            "#{@task['path_prefix']}#{@task['sequence_format']}#{@task['file_ext']}",
            Process.pid, Thread.current.object_id
          )
          if File.exist?(path)
            Embulk.logger.warn { "embulk-output-bigquery: unlink already existing #{path}" }
            File.unlink(path) rescue nil
          end
          Embulk.logger.info { "embulk-output-bigquery: create #{path}" }

          open(path, 'w')
        end

        def open(path, mode = 'w')
          file_io = File.open(path, mode)
          case @task['compression'].downcase
          when 'gzip'
            io = Zlib::GzipWriter.new(file_io)
          else
            io = file_io
          end
          self.class.mutex.synchronize do
            self.class.ios[path] = io
          end
          Thread.current[THREAD_LOCAL_IO_KEY] = io
        end

        def close
          io = thread_io
          io.close rescue nil
          io
        end

        def reopen
          io = thread_io
          open(io.path, 'a')
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
          io = thread_io
          # I once tried to split IO writing into another IO thread using SizedQueue
          # However, it resulted in worse performance, so I removed the codes.
          page.each do |record|
            Embulk.logger.trace { "embulk-output-bigquery: record #{record}" }
            formatted_record = @formatter_proc.call(record)
            Embulk.logger.trace { "embulk-output-bigquery: formatted_record #{formatted_record.chomp}" }
            io.write formatted_record
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

        def commit
          task_report = {
            'num_input_rows' => @num_input_rows,
          }
        end
      end
    end
  end
end
