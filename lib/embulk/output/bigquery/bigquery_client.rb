require 'google/apis/bigquery_v2'
require 'json'
require 'thwait'
require_relative 'google_client'
require_relative 'helper'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class BigqueryClient < GoogleClient
        BIGQUERY_TABLE_OPERATION_INTERVAL = 2 # https://cloud.google.com/bigquery/quotas

        def initialize(task, schema, fields = nil)
          scope = "https://www.googleapis.com/auth/bigquery"
          client_class = Google::Apis::BigqueryV2::BigqueryService
          super(task, scope, client_class)

          @schema = schema
          reset_fields(fields) if fields
          @project = @task['project']
          @dataset = @task['dataset']
          @location = @task['location']
          @location_for_log = @location.nil? ? 'us/eu' : @location

          @task['source_format'] ||= 'CSV'
          @task['max_bad_records'] ||= 0
          @task['field_delimiter'] ||= ','
          @task['source_format'] == 'CSV' ? @task['field_delimiter'] : nil
          @task['encoding'] ||= 'UTF-8'
          @task['ignore_unknown_values'] = false if @task['ignore_unknown_values'].nil?
          @task['allow_quoted_newlines'] = false if @task['allow_quoted_newlines'].nil?
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

        def with_job_retry(&block)
          retries = 0
          begin
            yield
          rescue BackendError, InternalError, RateLimitExceeded => e
            if e.is_a?(RateLimitExceeded)
              sleep(BIGQUERY_TABLE_OPERATION_INTERVAL)
            end

            if retries < @task['retries']
              retries += 1
              Embulk.logger.warn { "embulk-output-bigquery: retry \##{retries}, #{e.message}" }
              retry
            else
              Embulk.logger.error { "embulk-output-bigquery: retry exhausted \##{retries}, #{e.message}" }
              raise e
            end
          end
        end

        # @params gcs_patsh [Array] arary of gcs paths such as gs://bucket/path
        # @return [Array] responses
        def load_from_gcs(object_uris, table)
          with_job_retry do
            begin
              # As https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects#managingjobs says,
              # we should generate job_id in client code, otherwise, retrying would cause duplication
              if @task['prevent_duplicate_insert'] and (@task['mode'] == 'append' or @task['mode'] == 'append_direct')
                job_id = Helper.create_load_job_id(@task, path, fields)
              else
                job_id = "embulk_load_job_#{SecureRandom.uuid}"
              end
              Embulk.logger.info { "embulk-output-bigquery: Load job starting... job_id:[#{job_id}] #{object_uris} => #{@project}:#{@dataset}.#{table} in #{@location_for_log}" }

              body = {
                job_reference: {
                  project_id: @project,
                  job_id: job_id,
                },
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
                    source_uris: object_uris,
                  }
                }
              }

              if @location
                body[:job_reference][:location] = @location
              end
              
              if @task['schema_update_options']
                body[:configuration][:load][:schema_update_options] = @task['schema_update_options']
              end
              
              opts = {}

              Embulk.logger.debug { "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts})" }
              response = with_network_retry { client.insert_job(@project, body, opts) }
              unless @task['is_skip_job_result_check']
                response = wait_load('Load', response)
              end
              [response]
            rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
              response = {status_code: e.status_code, message: e.message, error_class: e.class}
              Embulk.logger.error {
                "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts}), response:#{response}"
              }
              raise Error, "failed to load #{object_uris} to #{@project}:#{@dataset}.#{table} in #{@location_for_log}, response:#{response}"
            end
          end
        end

        def load_in_parallel(paths, table)
          return [] if paths.empty?
          # You may think as, load job is a background job, so sending requests in parallel
          # does not improve performance. However, with actual experiments, this parallel
          # loadings drastically shortened waiting time. It looks one jobs.insert takes about 50 sec.
          # NOTICE: parallel uploadings of files consumes network traffic. With 24 concurrencies
          # with 100MB files consumed about 500Mbps in the experimented environment at a peak.
          #
          # We before had a `max_load_parallels` option, but this was not extensible for map reduce executor
          # So, we dropped it. See https://github.com/embulk/embulk-output-bigquery/pull/35
          responses = []
          threads = []
          Embulk.logger.debug { "embulk-output-bigquery: LOAD IN PARALLEL #{paths}" }
          paths.each_with_index do |path, idx|
            threads << Thread.new(path, idx) do |path, idx|
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
          responses
        end

        def load(path, table, write_disposition: 'WRITE_APPEND')
          with_job_retry do
            begin
              if File.exist?(path)
                # As https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects#managingjobs says,
                # we should generate job_id in client code, otherwise, retrying would cause duplication
                if @task['prevent_duplicate_insert'] and (@task['mode'] == 'append' or @task['mode'] == 'append_direct')
                  job_id = Helper.create_load_job_id(@task, path, fields)
                else
                  job_id = "embulk_load_job_#{SecureRandom.uuid}"
                end
                Embulk.logger.info { "embulk-output-bigquery: Load job starting... job_id:[#{job_id}] #{path} => #{@project}:#{@dataset}.#{table} in #{@location_for_log}" }
              else
                Embulk.logger.info { "embulk-output-bigquery: Load job starting... #{path} does not exist, skipped" }
                return
              end

              body = {
                job_reference: {
                  project_id: @project,
                  job_id: job_id,
                },
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
                    write_disposition:     write_disposition,
                    source_format:         @task['source_format'],
                    max_bad_records:       @task['max_bad_records'],
                    field_delimiter:       @task['source_format'] == 'CSV' ? @task['field_delimiter'] : nil,
                    encoding:              @task['encoding'],
                    ignore_unknown_values: @task['ignore_unknown_values'],
                    allow_quoted_newlines: @task['allow_quoted_newlines'],
                  }
                }
              }

              if @location
                body[:job_reference][:location] = @location
              end

              if @task['schema_update_options']
                body[:configuration][:load][:schema_update_options] = @task['schema_update_options']
              end

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
              response = with_network_retry { client.insert_job(@project, body, opts) }
              if @task['is_skip_job_result_check']
                response
              else
                response = wait_load('Load', response)
              end
            rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
              response = {status_code: e.status_code, message: e.message, error_class: e.class}
              # https://cloud.google.com/bigquery/troubleshooting-errors
              if @task['ignore_duplicate_error'] && e.status_code == 409
                Embulk.logger.warn {
                  "embulk-output-bigquery: Skip duplicated insert_job(#{@project}, #{body}, #{opts}), response:#{response}"
                }
              else
                Embulk.logger.error {
                  "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts}), response:#{response}"
                }
                raise Error, "failed to load #{path} to #{@project}:#{@dataset}.#{table} in #{@location_for_log}, response:#{response}"
              end
            end
          end
        end

        def copy(source_table, destination_table, destination_dataset = nil, write_disposition: 'WRITE_TRUNCATE')
          with_job_retry do
            begin
              destination_dataset ||= @dataset
              job_id = "embulk_copy_job_#{SecureRandom.uuid}"

              Embulk.logger.info {
                "embulk-output-bigquery: Copy job starting... job_id:[#{job_id}] " \
                "#{@project}:#{@dataset}.#{source_table} => #{@project}:#{destination_dataset}.#{destination_table}"
              }

              body = {
                job_reference: {
                  project_id: @project,
                  job_id: job_id,
                },
                configuration: {
                  copy: {
                    create_deposition: 'CREATE_IF_NEEDED',
                    write_disposition: write_disposition,
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

              if @location
                body[:job_reference][:location] = @location
              end

              opts = {}
              Embulk.logger.debug { "embulk-output-bigquery: insert_job(#{@project}, #{body}, #{opts})" }
              response = with_network_retry { client.insert_job(@project, body, opts) }
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
                "embulk-output-bigquery: #{kind} job completed... " \
                "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
              }
              break
            elsif elapsed.to_i > max_polling_time
              message = "embulk-output-bigquery: #{kind} job checking... " \
                "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[TIMEOUT]"
              Embulk.logger.info { message }
              raise JobTimeoutError.new(message)
            else
              Embulk.logger.info {
                "embulk-output-bigquery: #{kind} job checking... " \
                "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
              }
              sleep wait_interval
              _response = with_network_retry { client.get_job(@project, job_id, location: @location) }
            end
          end

          # cf. http://www.rubydoc.info/github/google/google-api-ruby-client/Google/Apis/BigqueryV2/JobStatus#errors-instance_method
          # `errors` returns Array<Google::Apis::BigqueryV2::ErrorProto> if any error exists.
          # Otherwise, this returns nil.
          if _errors = _response.status.errors
            msg = "failed during waiting a #{kind} job, get_job(#{@project}, #{job_id}), errors:#{_errors.map(&:to_h)}"
            if _errors.any? {|error| error.reason == 'backendError' }
              raise BackendError, msg
            elsif _errors.any? {|error| error.reason == 'internalError' }
              raise InternalError, msg
            elsif _errors.any? {|error| error.reason == 'rateLimitExceeded' }
              raise RateLimitExceeded, msg
            else
              Embulk.logger.error { "embulk-output-bigquery: #{msg}" }
              raise Error, msg
            end
          end

          Embulk.logger.info { "embulk-output-bigquery: #{kind} job response... job_id:[#{job_id}] response.statistics:#{_response.statistics.to_h}" }

          _response
        end

        def create_dataset(dataset = nil, reference: nil)
          dataset ||= @dataset
          begin
            Embulk.logger.info { "embulk-output-bigquery: Create dataset... #{@project}:#{dataset} in #{@location_for_log}" }
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
            if @location
              body[:location] = @location
            end
            opts = {}
            Embulk.logger.debug { "embulk-output-bigquery: insert_dataset(#{@project}, #{dataset}, #{@location_for_log}, #{body}, #{opts})" }
            with_network_retry { client.insert_dataset(@project, body, opts) }
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 409 && /Already Exists:/ =~ e.message
              # ignore 'Already Exists' error
              return
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_dataset(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create dataset #{@project}:#{dataset} in #{@location_for_log}, response:#{response}"
          end
        end

        def get_dataset(dataset = nil)
          dataset ||= @dataset
          begin
            Embulk.logger.info { "embulk-output-bigquery: Get dataset... #{@project}:#{dataset}" }
            with_network_retry { client.get_dataset(@project, dataset) }
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

        def create_table(table, dataset: nil, options: nil)
          begin
            dataset ||= @dataset
            options ||= {}
            options['time_partitioning'] ||= @task['time_partitioning']
            if Helper.has_partition_decorator?(table)
              options['time_partitioning'] ||= {'type' => 'DAY'}
              table = Helper.chomp_partition_decorator(table)
            end

            Embulk.logger.info { "embulk-output-bigquery: Create table... #{@project}:#{dataset}.#{table}" }
            body = {
              table_reference: {
                table_id: table,
              },
              schema: {
                fields: fields,
              }
            }

            if options['time_partitioning']
              body[:time_partitioning] = {
                type: options['time_partitioning']['type'],
                expiration_ms: options['time_partitioning']['expiration_ms'],
                field: options['time_partitioning']['field'],
                requirePartitionFilter: options['time_partitioning']['requirePartitionFilter'],
              }
            end

            opts = {}
            Embulk.logger.debug { "embulk-output-bigquery: insert_table(#{@project}, #{dataset}, #{@location_for_log}, #{body}, #{opts})" }
            with_network_retry { client.insert_table(@project, dataset, body, opts) }
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 409 && /Already Exists:/ =~ e.message
              # ignore 'Already Exists' error
              return
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_table(#{@project}, #{dataset}, #{@location_for_log}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create table #{@project}:#{dataset}.#{table} in #{@location_for_log}, response:#{response}"
          end
        end

        def delete_table(table, dataset: nil)
          begin
            table = Helper.chomp_partition_decorator(table)
            dataset ||= @dataset
            Embulk.logger.info { "embulk-output-bigquery: Delete table... #{@project}:#{dataset}.#{table}" }
            with_network_retry { client.delete_table(@project, dataset, table) }
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404 && /Not found:/ =~ e.message
              # ignore 'Not Found' error
              return
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: delete_table(#{@project}, #{dataset}, #{table}), response:#{response}"
            }
            raise Error, "failed to delete table #{@project}:#{dataset}.#{table}, response:#{response}"
          end
        end

        def get_table(table, dataset: nil)
          begin
            table = Helper.chomp_partition_decorator(table)
            dataset ||= @dataset
            Embulk.logger.info { "embulk-output-bigquery: Get table... #{@project}:#{dataset}.#{table}" }
            with_network_retry { client.get_table(@project, dataset, table) }
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404
              raise NotFoundError, "Table #{@project}:#{dataset}.#{table} is not found"
            end

            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: get_table(#{@project}, #{dataset}, #{table}), response:#{response}"
            }
            raise Error, "failed to get table #{@project}:#{dataset}.#{table}, response:#{response}"
          end
        end

        # Is this only a way to drop partition?
        def delete_partition(table_with_partition, dataset: nil)
          dataset ||= @dataset
          begin
            table = Helper.chomp_partition_decorator(table_with_partition)
            get_table(table, dataset: dataset)
          rescue NotFoundError
          else
            Embulk.logger.info { "embulk-output-bigquery: Delete partition... #{@project}:#{dataset}.#{table_with_partition}" }
            Tempfile.create('embulk_output_bigquery_empty_file_') do |fp|
              load(fp.path, table_with_partition, write_disposition: 'WRITE_TRUNCATE')
            end
          end
        end
      end
    end
  end
end
