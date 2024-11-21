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
      "float64" => Polars::Float64,
      "decimal" => Polars::Decimal,
      "boolean" => Polars::Boolean,
      "date" => Polars::Date,
      "datetime_ms" => Polars::Datetime.new("ms"),
      "datetime_us" => Polars::Datetime.new("us"),
      "datetime_ns" => Polars::Datetime.new("ns")
      # "string" => Polars::String,
      # "binary" => Polars::Binary
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

      assert_equal "byte", types["uint8"]
      assert_equal "short", types["uint16"]
      assert_equal "integer", types["uint32"]
      assert_equal "long", types["uint64"]

      assert_equal "float", types["float32"]
      assert_equal "double", types["float64"]

      assert_equal "decimal(38,0)", types["decimal"]
      assert_equal "boolean", types["boolean"]
      assert_equal "date", types["date"]
      assert_equal "timestamp_ntz", types["datetime_ms"]
      assert_equal "timestamp_ntz", types["datetime_us"]
      assert_equal "timestamp_ntz", types["datetime_ns"]

      # assert_equal "string", types["string"]
      # assert_equal "binary", types["binary"]

      pl_types = dt.to_polars.schema

      assert_equal Polars::Int8, pl_types["int8"]
      assert_equal Polars::Int16, pl_types["int16"]
      assert_equal Polars::Int32, pl_types["int32"]
      assert_equal Polars::Int64, pl_types["int64"]

      # unsigned integers are converted to signed
      assert_equal Polars::Int8, pl_types["uint8"]
      assert_equal Polars::Int16, pl_types["uint16"]
      assert_equal Polars::Int32, pl_types["uint32"]
      assert_equal Polars::Int64, pl_types["uint64"]

      assert_equal Polars::Float32, pl_types["float32"]
      assert_equal Polars::Float64, pl_types["float64"]

      assert_equal Polars::Decimal.new(38, 0), pl_types["decimal"]
      assert_equal Polars::Boolean, pl_types["boolean"]
      assert_equal Polars::Date, pl_types["date"]
      assert_equal Polars::Datetime.new("us"), pl_types["datetime_ms"]
      assert_equal Polars::Datetime.new("us"), pl_types["datetime_us"]
      assert_equal Polars::Datetime.new("us"), pl_types["datetime_ns"]
    end
  end

  def test_unsigned
    with_new_table do |table_uri|
      df = Polars::DataFrame.new({"a" => [255]}, schema: {"a" => Polars::UInt8})
      error = assert_raises(Polars::InvalidOperationError) do
        DeltaLake.write(table_uri, df)
      end
      assert_match "conversion from `u8` to `i8` failed", error.message
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
