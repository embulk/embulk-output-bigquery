module Embulk
  class ConfigError < ::StandardError
    def initialize(message=nil)
      if message
        super(message.to_s)
      else
        super()
      end
    end
  end
end
