module Embulk
  class DataSource < Hash
    def initialize(hash={}, default=nil, &block)
      if default.nil?
        super(&block)
      else
        super(default)
      end
      hash.each {|key,value| self[key] = value }
    end

    def param(key, type, options={})
      if self.has_key?(key)
        v = self[key]
        value =
          case type
          when :integer
            begin
              Integer(v)
            rescue => e
              raise ConfigError.new e
            end
          when :float
            begin
              Float(v)
            rescue => e
              raise ConfigError.new e
            end
          when :string
            begin
              String(v).dup
            rescue => e
              raise ConfigError.new e
            end
          when :bool
            begin
              !!v  # TODO validation
            rescue => e
              raise ConfigError.new e
            end
          when :hash
            raise ConfigError.new "Invalid value for :hash" unless v.is_a?(Hash)
            DataSource.new.merge!(v)
          when :array
            raise ConfigError.new "Invalid value for :array" unless v.is_a?(Array)
            v.dup
          else
            unless type.respond_to?(:load)
              raise ArgumentError, "Unknown type #{type.to_s.dump}"
            end
            begin
              type.load(v)
            rescue => e
              raise ConfigError.new e
            end
          end

      elsif options.has_key?(:default)
        value = options[:default]

      else
        raise ConfigError.new "Required field #{key.to_s.dump} is not set"
      end

      return value
    end
  end
end
