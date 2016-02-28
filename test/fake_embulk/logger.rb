require 'logger'

module Embulk
  class StandardLoggerAdapter < ::Logger
    def initialize(*args)
      super
      if RUBY_PLATFORM =~ /java/i
        self.formatter = lambda do |severity,datetime,progname,message|
          "#{datetime.strftime("%Y-%m-%d %H:%M:%S.%3N %z")} [#{severity}] (#{java.lang.Thread.currentThread.name}): #{message}\n"
        end
      else
        self.formatter = lambda do |severity,datetime,progname,message|
          "#{datetime.strftime("%Y-%m-%d %H:%M:%S.%3N %z")} [#{severity}]: #{message}\n"
        end
      end
    end

    def trace(message = nil, &block)
      debug(message, &block)
    end

    def trace?
      debug?
    end
  end

  def self.logger
    @@logger ||= StandardLoggerAdapter.new("/dev/null")
  end
end
