require 'uri'
require 'java'
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
            raise Error, "failed to insert bucket #{@project}:#{bucket}, response:#{response}"
          end
        end

        def insert_object(path, object: nil, bucket: nil)
          bucket ||= @bucket
          object ||= path
          object = object.start_with?('/') ? object[1..-1] : object
          object_uri = URI.join("gs://#{bucket}", object).to_s

          started = Time.now
          retries = 0
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
            # memo: gcs is strongly consistent for insert (read-after-write). ref: https://cloud.google.com/storage/docs/consistency
            client.insert_object(bucket, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to insert object #{@project}:#{object_uri}, response:#{response}"
          rescue ::Java::Java.net.SocketException => e
            # I encountered `java.net.SocketException: Broken pipe` serveral times
            # google-api-ruby-client itself has a retry feature, but it does not retry with SocketException
            if e.message == 'Broken pipe'
              if retries < @task['retries']
                response = {message: e.message, error_class: e.class}
                Embulk.logger.warn {
                  "embulk-output-bigquery: RETRY: insert_object(#{bucket}, #{body}, #{opts}), response:#{response}"
                }
                retries += 1 # want to share with google-api-ruby-client, but it is difficult
                retry
              end
            end
            raise e
          end
        end

        def insert_objects(paths, objects: nil, bucket: nil)
          return [] if paths.empty?
          bucket ||= @bucket
          objects ||= paths
          raise "number of paths and objects are different" if paths.size != objects.size

          responses = []
          paths.each_with_index do |path, idx|
            object = objects[idx]
            responses << insert_object(path, object: object, bucket: bucket)
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
