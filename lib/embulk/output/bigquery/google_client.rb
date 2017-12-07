require 'google/api_client/auth/key_utils'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class Error < StandardError; end
      class JobTimeoutError < Error; end
      class NotFoundError < Error; end
      class BackendError < Error; end
      class InternalError < Error; end
      class RateLimitExceeded < Error; end

      class GoogleClient
        def initialize(task, scope, client_class)
          @task = task
          @scope = scope
          @client_class = client_class
        end

        def client
          return @cached_client if @cached_client && @cached_client_expiration > Time.now

          client = @client_class.new
          client.client_options.application_name = @task['application_name']
          client.request_options.retries = @task['retries']
          if client.request_options.respond_to?(:timeout_sec)
            client.request_options.timeout_sec = @task['timeout_sec'] || 300
            client.request_options.open_timeout_sec = @task['open_timeout_sec'] || 300
          else # google-api-ruby-client >= v0.11.0
            if @task['timeout_sec']
              Embulk.logger.warn { "embulk-output-bigquery: timeout_sec is deprecated in google-api-ruby-client >= v0.11.0. Use read_timeout_sec instead" }
            end
            client.client_options.open_timeout_sec = @task['open_timeout_sec'] || 300 # default: 60
            client.client_options.send_timeout_sec = @task['send_timeout_sec'] || 300 # default: 120
            client.client_options.read_timeout_sec = @task['read_timeout_sec'] || @task['timeout_sec'] || 300 # default: 60
          end
          Embulk.logger.debug { "embulk-output-bigquery: client_options: #{client.client_options.to_h}" }
          Embulk.logger.debug { "embulk-output-bigquery: request_options: #{client.request_options.to_h}" }

          case @task['auth_method']
          when 'private_key'
            private_key_passphrase = 'notasecret'
            key = Google::APIClient::KeyUtils.load_from_pkcs12(@task['p12_keyfile'], private_key_passphrase)
            auth = Signet::OAuth2::Client.new(
              token_credential_uri: "https://accounts.google.com/o/oauth2/token",
              audience: "https://accounts.google.com/o/oauth2/token",
              scope: @scope,
              issuer: @task['service_account_email'],
              signing_key: key)

          when 'compute_engine'
            auth = Google::Auth::GCECredentials.new

          when 'json_key'
            json_key = @task['json_keyfile']
            if File.exist?(json_key)
              auth = File.open(json_key) do |f|
                Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: f, scope: @scope)
              end
            else
              key = StringIO.new(json_key)
              auth = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: @scope)
            end

          when 'application_default'
            auth = Google::Auth.get_application_default([@scope])

          else
            raise ConfigError, "Unknown auth method: #{@task['auth_method']}"
          end

          client.authorization = auth

          @cached_client_expiration = Time.now + 1800
          @cached_client = client
        end

        # google-api-ruby-client itself has a retry feature, but it does not retry with SocketException
        def with_network_retry(&block)
          retries = 0
          begin
            yield
          rescue ::Java::Java.net.SocketException, ::Java::Java.net.ConnectException => e
            if ['Broken pipe', 'Connection reset', 'Connection timed out'].include?(e.message)
              if retries < @task['retries']
                retries += 1
                Embulk.logger.warn { "embulk-output-bigquery: retry \##{retries}, #{e.class} #{e.message}" }
                retry
              else
                Embulk.logger.error { "embulk-output-bigquery: retry exhausted \##{retries}, #{e.class} #{e.message}" }
                raise e
              end
            else
              raise e
            end
          end
        end
      end
    end
  end
end
