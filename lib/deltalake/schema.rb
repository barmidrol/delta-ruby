module DeltaLake
  class Schema
    attr_reader :fields

    def initialize(fields)
      @fields = fields
    end
  end
end
