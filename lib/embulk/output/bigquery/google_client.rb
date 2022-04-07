require_relative 'auth'

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
          @auth = Auth.new(task, scope)
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

          client.authorization = @auth.authenticate

          @cached_client_expiration = Time.now + 1800
          @cached_client = client
        end

        # google-api-ruby-client itself has a retry feature, but it does not retry with SocketException
        def with_network_retry(&block)
          retries = 0
          begin
            yield

            # httpclient which google-api-ruby-client depends on, catches java.net.SocketException and java.net.ConnectionException and
            # raises SSLError.
            # https://github.com/nahi/httpclient/blob/4658227a46f7caa633ef8036f073bbd1f0a955a2/lib/httpclient/jruby_ssl_socket.rb#L124-L134
          rescue OpenSSL::SSL::SSLError => e
            retry_messages = [
              "Java::JavaNet::SocketException: Connection reset",
              "Java::JavaNet::SocketException: Broken pipe",
              "Java::JavaNet::ConnectException: Connection timed out",
            ]
            if retry_messages.include?(e.message)
              if retries < @task['retries']
                retries += 1
                Embulk.logger.warn { "embulk-output-bigquery: retry \##{retries}, #{e.class} #{e.message}" }
                retry
              else
                Embulk.logger.error { "embulk-output-bigquery: retry exhausted \##{retries}, #{e.class} #{e.message}" }
                raise e
              end
            end
          end
        end
      end
    end
  end
end
