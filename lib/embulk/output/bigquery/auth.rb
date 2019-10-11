require 'googleauth'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class Auth

        attr_reader :auth_method, :json_key, :scope

        def initialize(task, scope)
          @auth_method = task['auth_method']
          @json_key = task['json_keyfile']
          @scope = scope
        end

        def authenticate
          case auth_method
          when 'authorized_user'
            key = StringIO.new(json_key)
            return Google::Auth::UserRefreshCredentials.make_creds(json_key_io: key, scope: scope)
          when 'compute_engine'
            return Google::Auth::GCECredentials.new
          when 'service_account', 'json_key' # json_key is for backward compatibility
            key = StringIO.new(json_key)
            return Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: scope)
          when 'application_default'
            return Google::Auth.get_application_default([scope])
          else
            raise ConfigError.new("Unknown auth method: #{auth_method}")
          end
        end
      end
    end
  end
end
