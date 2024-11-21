# ext
begin
  require "deltalake/#{RUBY_VERSION.to_f}/deltalake"
rescue LoadError
  require "deltalake/deltalake"
end

# stdlib
require "json"

# modules
require_relative "deltalake/field"
require_relative "deltalake/metadata"
require_relative "deltalake/schema"
require_relative "deltalake/table"
require_relative "deltalake/version"

module DeltaLake
  class Error < StandardError; end
  class TableNotFoundError < Error; end
  class DeltaProtocolError < Error; end
  class CommitFailedError < Error; end
  class SchemaMismatchError < Error; end

  class << self
    def write(
      table_or_uri,
      data,
      partition_by: nil,
      mode: "error",
      name: nil,
      description: nil,
      configuration: nil,
      schema_mode: nil,
      storage_options: nil,
      predicate: nil,
      target_file_size: nil
    )
      table, table_uri = try_get_table_and_table_uri(table_or_uri, storage_options)

      if partition_by.is_a?(String)
        partition_by = [partition_by]
      end

      if !table.nil? && mode == "ignore"
        return
      end

      data = convert_data(data)

      write_deltalake_rust(
        table_uri,
        data,
        mode,
        table&._table,
        schema_mode,
        partition_by,
        predicate,
        target_file_size,
        name,
        description,
        configuration,
        storage_options
      )

      if table
        table.update_incremental
      end
    end

    private

    def try_get_table_and_table_uri(table_or_uri, storage_options)
      if !table_or_uri.is_a?(String) && !table_or_uri.is_a?(Table)
        raise ArgumentError, "table_or_uri must be a String or Table"
      end

      if table_or_uri.is_a?(String)
        table = try_get_deltatable(table_or_uri, storage_options)
        table_uri = table_or_uri.to_s
      else
        table = table_or_uri
        table_uri = table._table.table_uri
      end

      [table, table_uri]
    end

    def try_get_deltatable(table_uri, storage_options)
      Table.new(table_uri, storage_options: storage_options)
    rescue TableNotFoundError
      nil
    end

    def convert_data(data)
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
    def convert_polars_data(data)
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

    def convert_polars_type(t)
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
      end
    end
  end
end
