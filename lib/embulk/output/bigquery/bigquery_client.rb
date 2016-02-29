require 'google/apis/bigquery_v2'
require 'google/api_client/auth/key_utils'
require 'json'
require 'thwait'
require_relative 'helper'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class Error < StandardError; end
      class JobTimeoutError < Error; end
      class NotFoundError < Error; end

      class BigqueryClient
        def initialize(task, schema, fields = nil)
          @task = task
          @schema = schema

          @auth_method = task['auth_method']
          @private_key_path = task['p12_keyfile']
          @private_key_passphrase = 'notasecret'
          @json_key = task['json_keyfile']

          @project = task['project']
          @dataset = task['dataset']

          reset_fields(fields) if fields
        end

        def client
          return @cached_client if @cached_client && @cached_client_expiration > Time.now

          client = Google::Apis::BigqueryV2::BigqueryService.new
          client.client_options.application_name = @task['application_name']
          client.request_options.retries = @task['retries']
          client.request_options.timeout_sec = @task['timeout_sec']
          client.request_options.open_timeout_sec = @task['open_timeout_sec']
          Embulk.logger.debug { "embulk-output-bigquery: client_options: #{client.client_options.to_h}" }
          Embulk.logger.debug { "embulk-output-bigquery: request_options: #{client.request_options.to_h}" }

          scope = "https://www.googleapis.com/auth/bigquery"

          case @auth_method
          when 'private_key'
            key = Google::APIClient::KeyUtils.load_from_pkcs12(@private_key_path, @private_key_passphrase)
            auth = Signet::OAuth2::Client.new(
              token_credential_uri: "https://accounts.google.com/o/oauth2/token",
              audience: "https://accounts.google.com/o/oauth2/token",
              scope: scope,
              issuer: @email,
              signing_key: key)

          when 'compute_engine'
            auth = Google::Auth::GCECredentials.new

          when 'json_key'
            if File.exist?(@json_key)
              auth = File.open(@json_key) do |f|
                Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: f, scope: scope)
              end
            else
              key = StringIO.new(@json_key)
              auth = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: scope)
            end

          when 'application_default'
            auth = Google::Auth.get_application_default([scope])

          else
            raise ConfigError, "Unknown auth method: #{@auth_method}"
          end

          client.authorization = auth

          @cached_client_expiration = Time.now + 1800
          @cached_client = client
        end

        def fields
          return @fields if @fields
          if @task['schema_file']
            @fields = Helper.deep_symbolize_keys(JSON.parse(File.read(@task['schema_file'])))
          elsif @task['template_table']
            @fields = fields_from_table(@task['template_table'])
          else
            @fields = Helper.fields_from_embulk_schema(@task, @schema)
          end
        end

        def fields_from_table(table)
          response = get_table(table)
          response.schema.fields.map {|field| field.to_h }
        end

        def reset_fields(fields = nil)
          @fields = fields
          self.fields
        end

        def load_in_parallel(paths, table)
          return [] if paths.empty?
          # You may think as, load job is a background job, so sending requests in parallel
          # does not improve performance. However, with actual experiments, this parallel
          # loadings drastically shortened waiting time. It looks one jobs.insert takes about 50 sec.
          # NOTICE: parallel uploadings of files consumes network traffic. With 24 concurrencies
          # with 100MB files consumed about 500Mbps in the experimented environment at a peak.
          max_load_parallels = @task['max_load_parallels'] || paths.size
          responses = []
          paths.each_with_index.each_slice(max_load_parallels) do |paths_group|
            Embulk.logger.debug { "embulk-output-bigquery: LOAD IN PARALLEL #{paths_group}" }
            threads = []
            paths_group.each do |path, idx|
              threads << Thread.new do
                # I am not sure whether google-api-ruby-client is thread-safe,
                # so let me create new instances for each thread for safe
                bigquery = self.class.new(@task, @schema, fields)
                response = bigquery.load(path, table)
                [idx, response]
              end
            end
            ThreadsWait.all_waits(*threads) do |th|
              idx, response = th.value # raise errors occurred in threads
              responses[idx] = response
            end
          end
          responses
        end

        def load(path, table)
          begin
            if File.exist?(path)
              Embulk.logger.info { "embulk-output-bigquery: Load job starting... #{path} => #{@project}:#{@dataset}.#{table}" }
            else
              Embulk.logger.info { "embulk-output-bigquery: Load job starting... #{path} does not exist, skipped" }
              return
            end

            if @task['prevent_duplicate_insert']
              job_reference = {
                job_reference: {
                  project_id: @project,
                  job_id: Helper.create_job_id(@task, path, table, fields),
                }
              }
            else
              job_reference = {}
            end

            body = {
              configuration: {
                load: {
                  destination_table: {
                    project_id: @project,
                    dataset_id: @dataset,
                    table_id: table,
                  },
                  schema: {
                    fields: fields,
                  },
                  write_disposition: 'WRITE_APPEND',
                  source_format:         @task['source_format'],
                  max_bad_records:       @task['max_bad_records'],
                  field_delimiter:       @task['source_format'] == 'CSV' ? @task['field_delimiter'] : nil,
                  encoding:              @task['encoding'],
                  ignore_unknown_values: @task['ignore_unknown_values'],
                  allow_quoted_newlines: @task['allow_quoted_newlines'],
                }
              }.merge!(job_reference)
            }
            opts = {
              upload_source: path,
              content_type: "application/octet-stream",
              # options: {
              #   retries: @task['retries'],
              #   timeout_sec: @task['timeout_sec'],
              #   open_timeout_sec: @task['open_timeout_sec']
              # },
            }
            Embulk.logger.debug { "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts})" }
            response = client.insert_job(@project, body, opts)
            unless @task['is_skip_job_result_check']
              wait_load('Load', response)
            end
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to load #{path} to #{@project}:#{@dataset}.#{table}, response:#{response}"
          end
        end

        def copy(source_table, destination_table, destination_dataset = nil)
          begin
            destination_dataset ||= @dataset
            Embulk.logger.info {
              "embulk-output-bigquery: Copy job starting... " \
              "#{@project}:#{@dataset}.#{source_table} => #{@project}:#{destination_dataset}.#{destination_table}"
            }
            body = {
              configuration: {
                copy: {
                  create_deposition: 'CREATE_IF_NEEDED',
                  write_disposition: 'WRITE_TRUNCATE',
                  source_table: {
                    project_id: @project,
                    dataset_id: @dataset,
                    table_id: source_table,
                  },
                  destination_table: {
                    project_id: @project,
                    dataset_id: destination_dataset,
                    table_id: destination_table,
                  },
                }
              }
            }
            opts = {}
            Embulk.logger.debug { "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts})" }
            response = client.insert_job(@project, body, opts)
            wait_load('Copy', response)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to copy #{@project}:#{@dataset}.#{source_table} " \
              "to #{@project}:#{destination_dataset}.#{destination_table}, response:#{response}"
          end
        end

        def wait_load(kind, response)
          started = Time.now

          wait_interval = @task['job_status_polling_interval']
          max_polling_time = @task['job_status_max_polling_time']
          _response = response

          while true
            job_id = _response.job_reference.job_id
            elapsed = Time.now - started
            status = _response.status.state
            if status == "DONE"
              Embulk.logger.info {
                "embulk-output-bigquery: #{kind} job completed successfully... " \
                "job id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
              }
              break
            elsif elapsed.to_i > max_polling_time
              message = "embulk-output-bigquery: Checking #{kind} job status... " \
                "job id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[TIMEOUT]"
              Embulk.logger.info { message }
              raise JobTimeoutError.new(message)
            else
              Embulk.logger.info {
                "embulk-output-bigquery: Checking #{kind} job status... " \
                "job id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
              }
              sleep wait_interval
              _response = client.get_job(@project, job_id)
            end
          end

          if error_result = _response.status.error_result
            Embulk.logger.error {
              "embulk-output-bigquery: get_job(#{@project}, #{job_id}), " \
              "error_result:#{error_result.to_h}"
            }
            raise Error, "failed during waiting a job, error_result:#{error_result.to_h}"
          end

          _response
        end

        def create_dataset(dataset = nil, reference: nil)
          dataset ||= @dataset
          begin
            Embulk.logger.info { "embulk-output-bigquery: Create dataset... #{@project}:#{dataset}" }
            hint = {}
            if reference
              response = get_dataset(reference)
              hint = { access: response.access }
            end
            body = {
              dataset_reference: {
                project_id: @project,
                dataset_id: dataset,
              },
            }.merge(hint)
            opts = {}
            Embulk.logger.debug { "embulk-output-bigquery: insert_dataset(#{@project}, #{dataset}, #{body}, #{opts})" }
            client.insert_dataset(@project, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 409 && /Already Exists:/ =~ e.message
              # ignore 'Already Exists' error
              return
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_dataset(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create dataset #{@project}:#{dataset}, response:#{response}"
          end
        end

        def get_dataset(dataset = nil)
          dataset ||= @dataset
          begin
            Embulk.logger.info { "embulk-output-bigquery: Get dataset... #{@project}:#{@dataset}" }
            client.get_dataset(@project, dataset)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404
              raise NotFoundError, "Dataset #{@project}:#{dataset} is not found"
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: get_dataset(#{@project}, #{dataset}), response:#{response}"
            }
            raise Error, "failed to get dataset #{@project}:#{dataset}, response:#{response}"
          end
        end

        def create_table(table)
          begin
            Embulk.logger.info { "embulk-output-bigquery: Create table... #{@project}:#{@dataset}.#{table}" }
            body = {
              table_reference: {
                table_id: table,
              },
              schema: {
                fields: fields,
              }
            }
            opts = {}
            Embulk.logger.debug { "embulk-output-bigquery: insert_table(#{@project}, #{@dataset}, #{body}, #{opts})" }
            client.insert_table(@project, @dataset, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 409 && /Already Exists:/ =~ e.message
              # ignore 'Already Exists' error
              return
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_table(#{@project}, #{@dataset}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create table #{@project}:#{@dataset}.#{table}, response:#{response}"
          end
        end

        def delete_table(table)
          begin
            Embulk.logger.info { "embulk-output-bigquery: Delete table... #{@project}:#{@dataset}.#{table}" }
            client.delete_table(@project, @dataset, table)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404 && /Not found:/ =~ e.message
              # ignore 'Not Found' error
              return
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: delete_table(#{@project}, #{@dataset}, #{table}), response:#{response}"
            }
            raise Error, "failed to delete table #{@project}:#{@dataset}.#{table}, response:#{response}"
          end
        end

        def get_table(table)
          begin
            Embulk.logger.info { "embulk-output-bigquery: Get table... #{@project}:#{@dataset}.#{table}" }
            client.get_table(@project, @dataset, table)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404
              raise NotFoundError, "Table #{@project}:#{@dataset}.#{table} is not found"
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: get_table(#{@project}, #{@dataset}, #{table}), response:#{response}"
            }
            raise Error, "failed to get table #{@project}:#{@dataset}.#{table}, response:#{response}"
          end
        end
      end
    end
  end
end
