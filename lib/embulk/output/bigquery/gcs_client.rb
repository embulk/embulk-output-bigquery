require 'uri'
require 'digest/md5'
require 'base64'
require 'google/apis/storage_v1'
require_relative 'google_client'
require_relative 'helper'

# ToDo: Use https://cloud.google.com/storage/docs/streaming if google-api-ruby-client supports streaming transfers
# ToDo: Tests are not written because this implementation will probably entirely changed on supporting streaming transfers
module Embulk
  module Output
    class Bigquery < OutputPlugin
      class GcsClient < GoogleClient
        def initialize(task)
          scope = "https://www.googleapis.com/auth/cloud-platform"
          client_class = Google::Apis::StorageV1::StorageService
          super(task, scope, client_class)

          @project = @task['project']
          @bucket = @task['gcs_bucket']
        end

        def insert_bucket(bucket = nil)
          bucket ||= @bucket
          begin
            Embulk.logger.info { "embulk-output-bigquery: Insert bucket... #{@project}:#{bucket}" }
            body  = {
              name: bucket,
            }
            opts = {}

            Embulk.logger.debug { "embulk-output-bigquery: insert_bucket(#{@project}, #{body}, #{opts})" }
            client.insert_bucket(@project, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 409 && /conflict:/ =~ e.message
              # ignore 'Already Exists' error
              return nil
            end
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_bucket(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create bucket #{@project}:#{bucket}, response:#{response}"
          end
        end

        def insert_object(path, object: nil, bucket: nil)
          bucket ||= @bucket
          object ||= path
          object = object.start_with?('/') ? object[1..-1] : object
          object_uri = URI.join("gs://#{bucket}", object).to_s

          started = Time.now
          begin
            Embulk.logger.info { "embulk-output-bigquery: Insert object... #{path} => #{@project}:#{object_uri}" }
            body = {
              name: object,
            }
            opts = {
              upload_source: path,
              content_type: 'application/octet-stream'
            }

            Embulk.logger.debug { "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts})" }
            client.insert_object(bucket, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to insert object #{@project}:#{object_uri}, response:#{response}"
          end

          wait_insert_object(path, object: object, bucket: nil, started: started)
        end

        # GCS is eventually consistent. Make sure file was writtern surely
        private def wait_insert_object(path, object: nil, bucket: nil, started: nil)
          bucket ||= @bucket
          object ||= path
          started ||= Time.now

          gcs_path = "gs://#{File.join(bucket, object)}"
          wait_interval = @task['job_status_polling_interval']
          max_polling_time = @task['job_status_max_polling_time']

          md5_hash = Base64.encode64(Digest::MD5.file(path).digest).chomp
          log_head = "embulk-output-bigquery: Insert object checking... path:[#{gcs_path}]"
          while true
            begin
              elapsed = Time.now - started
              response = client.get_object(bucket, object, {})
              if response.md5_hash.to_s == md5_hash
                Embulk.logger.info { "#{log_head} elapsed_time:#{elapsed.to_f}sec status:[DONE]" }
                break
              elsif elapsed.to_i > max_polling_time
                message = "#{log_head} elapsed_time:#{elapsed.to_f}sec status:[TIMEOUT]"
                Embulk.logger.info { message }
                raise JobTimeoutError.new(message)
              else
                Embulk.logger.info { "#{log_head} elapsed_time:#{elapsed.to_f}sec status:[PROCESSING]" }
                sleep wait_interval
              end
            rescue Google::Apis::ClientError => e
              if e.status_code == 404
                Embulk.logger.info { "#{log_head} elapsed_time:#{elapsed.to_f}sec status:[PROCESSING]" }
                sleep wait_interval
              else
                raise e
              end
            rescue Google::Apis::ServerError => e
              Embulk.logger.info { "#{log_head} elapsed_time:#{elapsed.to_f}sec status:[SERVER_ERROR]" }
              sleep wait_interval
            end
          end
        end

        def insert_objects_in_parallel(paths, objects: nil, bucket: nil)
          return [] if paths.empty?
          bucket ||= @bucket
          objects ||= paths
          raise "number of paths and objects are different" if paths.size != objects.size

          responses = []
          threads = []
          Embulk.logger.debug { "embulk-output-bigquery: INSERT OBJECTS IN PARALLEL #{paths}" }
          paths.each_with_index do |path, idx|
            object = objects[idx]
            threads << Thread.new do
              # I am not sure whether google-api-ruby-client is thread-safe,
              # so let me create new instances for each thread for safe
              gcs = self.class.new(@task)
              response = gcs.insert_object(path, object: object, bucket: bucket)
              [idx, response]
            end
          end
          ThreadsWait.all_waits(*threads) do |th|
            idx, response = th.value # raise errors occurred in threads
            responses[idx] = response
          end
          responses
        end

        def delete_object(object, bucket: nil)
          bucket ||= @bucket
          object = object.start_with?('/') ? object[1..-1] : object
          object_uri = URI.join("gs://#{bucket}", object).to_s
          begin
            Embulk.logger.info { "embulk-output-bigquery: Delete object... #{@project}:#{object_uri}" }
            opts = {}

            Embulk.logger.debug { "embulk-output-bigquery: delete_object(#{bucket}, #{object}, #{opts})" }
            response = client.delete_object(bucket, object, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404 # ignore 'notFound' error
              return nil
            end
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: delete_object(#{bucket}, #{object}, #{opts}), response:#{response}"
            }
            raise Error, "failed to delete object #{@project}:#{object_uri}, response:#{response}"
          end
        end
      end
    end
  end
end
