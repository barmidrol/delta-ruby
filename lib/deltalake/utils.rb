module DeltaLake
  module Utils
    def self.convert_data(data)
      if data.respond_to?(:arrow_c_stream)
        # TODO convert other object types
        # should probably move logic to Rust
        if defined?(Polars::DataFrame) && data.is_a?(Polars::DataFrame)
          data = convert_polars_data(data)
        end

        data.arrow_c_stream
      else
        raise TypeError, "Only objects implementing the Arrow C stream interface are valid inputs for source."
      end
    end

    # unsigned integers are not part of the protocol
    # https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types
    def self.convert_polars_data(data)
      new_schema = {}
      data.schema.each do |k, v|
        new_type = convert_polars_type(v)
        new_schema[k] = new_type if new_type
      end

      if new_schema.any?
        data.cast(new_schema)
      else
        data
      end
    end

    def self.convert_polars_type(t)
      case t
      when Polars::UInt8
        Polars::Int8
      when Polars::UInt16
        Polars::Int16
      when Polars::UInt32
        Polars::Int32
      when Polars::UInt64
        Polars::Int64
      when Polars::Datetime
        Polars::Datetime.new("us", t.time_zone) if t.time_unit != "us"
      when Polars::List
        inner = convert_polars_type(t.inner)
        Polars::List.new(inner) if inner
      when Polars::Array
        inner = convert_polars_type(t.inner)
        Polars::Array.new(t.inner, t.width) if inner
      when Polars::Struct
        if t.fields.any? { |f| convert_polars_type(f.dtype) }
          fields = t.fields.map { |f| Polars::Field.new(f.name, convert_polars_type(f.dtype) || f.dtype) }
          Polars::Struct.new(fields)
        end
      end
    end
  end
end
