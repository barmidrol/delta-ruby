require_relative "test_helper"

class WriteTest < Minitest::Test
  def test_mode
    with_new_table do |table_uri|
      df = Polars::DataFrame.new({"a" => [1, 2, 3]})
      DeltaLake.write(table_uri, df)

      dt = DeltaLake::Table.new(table_uri)
      assert_equal df, dt.to_polars
      assert_equal 0, dt.version

      error = assert_raises(DeltaLake::Error) do
        DeltaLake.write(dt, df)
      end
      assert_match "table already exists", error.message

      DeltaLake.write(dt, df, mode: "overwrite")
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version

      DeltaLake.write(dt, df, mode: "ignore")
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version

      DeltaLake.write(dt, df, mode: "append")
      assert_equal Polars.concat([df, df]), dt.to_polars
      assert_equal 2, dt.version

      dt.load_as_version(dt.version - 1)
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version

      dt = DeltaLake::Table.new(table_uri, version: 1)
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version
    end
  end

  def test_types
    schema = {
      "int8" => Polars::Int8,
      "int16" => Polars::Int16,
      "int32" => Polars::Int32,
      "int64" => Polars::Int64,
      "uint8" => Polars::UInt8,
      "uint16" => Polars::UInt16,
      "uint32" => Polars::UInt32,
      "uint64" => Polars::UInt64,
      "float32" => Polars::Float32,
      "float64" => Polars::Float64
    }
    row = {}
    schema.each_key do |k|
      row[k] = 1
    end
    df = Polars::DataFrame.new([row], schema: schema)
    with_table(df) do |dt|
      types = dt.schema.fields.to_h { |f| [f.name, f.type] }

      assert_equal "byte", types["int8"]
      assert_equal "short", types["int16"]
      assert_equal "integer", types["int32"]
      assert_equal "long", types["int64"]

      # unsigned numbers are not part of the protocol
      # https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types
      # https://github.com/delta-io/delta-rs/issues/1751
      # https://github.com/delta-io/delta-rs/issues/2175
      assert_equal "byte", types["uint8"]
      assert_equal "short", types["uint16"]
      assert_equal "integer", types["uint32"]
      assert_equal "long", types["uint64"]

      assert_equal "float", types["float32"]
      assert_equal "double", types["float64"]

      assert_equal schema, dt.to_polars.schema
    end
  end

  def test_invalid_data
    with_new_table do |table_uri|
      error = assert_raises(TypeError) do
        DeltaLake.write(table_uri, Object.new)
      end
      assert_equal "Only objects implementing the Arrow C stream interface are valid inputs for source.", error.message
    end
  end
end
