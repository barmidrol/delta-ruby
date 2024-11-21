module DeltaLake
  class Metadata
    def initialize(table)
      @metadata = table.metadata
    end

    def id
      @metadata.id
    end

    def name
      @metadata.name
    end

    def description
      @metadata.description
    end

    def partition_columns
      @metadata.partition_columns
    end

    def created_time
      @metadata.created_time
    end

    def configuration
      @metadata.configuration
    end

    def inspect
      attributes = {
        id: id,
        name: name,
        description: description,
        partition_columns: partition_columns,
        created_time: created_time,
        configuration: configuration
      }
      "<#{self.class.name} #{attributes.map { |k, v| "#{k}=#{v.inspect}" }.join(", ")}>"
    end
  end
end
