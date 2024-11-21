module DeltaLake
  class Field
    def inspect
      attributes = {
        name: name,
        type: type,
        nullable: nullable
      }
      "<#{self.class.name} #{attributes.map { |k, v| "#{k}=#{v.inspect}" }.join(", ")}>"
    end
  end
end
