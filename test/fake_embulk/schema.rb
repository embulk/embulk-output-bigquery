module Embulk
  class Schema < ::Array
    def initialize(*args)
      super
    end

    def names
      map {|c| c[:name] }
    end

    def types
      map {|c| c[:type] }
    end
  end
end
