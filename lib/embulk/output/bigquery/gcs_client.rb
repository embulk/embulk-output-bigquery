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
          @destination_project = @task['destination_project']
          @bucket = @task['gcs_bucket']
          @location = @task['location']
        end

        def insert_temporary_bucket(bucket = nil)
          bucket ||= @bucket
          begin
            Embulk.logger.info { "embulk-output-bigquery: Insert bucket... #{@destination_project}:#{bucket}" }
            body = {
              name: bucket,
              lifecycle: {
                rule: [
                  {
                    action: {
                      type: "Delete",
                    },
                    condition: {
                      age: 1,
                    }
                  },
                ]
              }
            }

            if @location
              body[:location] = @location
            end

            opts = {}

            Embulk.logger.debug { "embulk-output-bigquery: insert_temporary_bucket(#{@project}, #{body}, #{opts})" }
            with_network_retry { client.insert_bucket(@project, body, **opts) }
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 409 && /conflict:/ =~ e.message
              # ignore 'Already Exists' error
              return nil
            end
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_temporary_bucket(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to insert bucket #{@destination_project}:#{bucket}, response:#{response}"
          end
        end

        def insert_object(path, object: nil, bucket: nil)
          bucket ||= @bucket
          object ||= path
          object = object.start_with?('/') ? object[1..-1] : object
          object_uri = URI.join("gs://#{bucket}", object).to_s

          started = Time.now
          begin
            Embulk.logger.info { "embulk-output-bigquery: Insert object... #{path} => #{@destination_project}:#{object_uri}" }
            body = {
              name: object,
            }
            opts = {
              upload_source: path,
              content_type: 'application/octet-stream'
            }

            Embulk.logger.debug { "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts})" }
            # memo: gcs is strongly consistent for insert (read-after-write). ref: https://cloud.google.com/storage/docs/consistency
            with_network_retry { client.insert_object(bucket, body, **opts) }
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to insert object #{@destination_project}:#{object_uri}, response:#{response}"
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
            Embulk.logger.info { "embulk-output-bigquery: Delete object... #{@destination_project}:#{object_uri}" }
            opts = {}

            Embulk.logger.debug { "embulk-output-bigquery: delete_object(#{bucket}, #{object}, #{opts})" }
            response = with_network_retry { client.delete_object(bucket, object, **opts) }
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404 # ignore 'notFound' error
              return nil
            end
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: delete_object(#{bucket}, #{object}, #{opts}), response:#{response}"
            }
            raise Error, "failed to delete object #{@destination_project}:#{object_uri}, response:#{response}"
          end
        end
      end
    end
  end
end
