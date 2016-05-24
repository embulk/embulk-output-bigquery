require 'uri'
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
