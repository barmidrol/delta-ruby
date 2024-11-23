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

      time = Time.now
      # version timestamps are stored at ms precision
      # extra ms needed for CI
      sleep(0.002)

      DeltaLake.write(dt, df, mode: "ignore")
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version

      DeltaLake.write(dt, df, mode: "append")
      assert_equal Polars.concat([df, df]), dt.to_polars
      assert_equal 2, dt.version

      assert_empty dt.transaction_versions

      dt.load_as_version(dt.version - 1)
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version

      dt.load_as_version(time)
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version

      dt = DeltaLake::Table.new(table_uri, version: 1)
      assert_equal df, dt.to_polars
      assert_equal 1, dt.version
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
