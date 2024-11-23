require_relative "test_helper"

class MergeTest < Minitest::Test
  def test_when_matched_update
    df = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 6]})
    with_table(df) do |dt|
      source = Polars::DataFrame.new({"x" => [2, 3], "y" => [5, 8]})

      dt.merge(source, "target.x = source.x", source_alias: "source", target_alias: "target")
        .when_matched_update({"x" => "source.x", "y" => "source.y"})
        .execute

      expected = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 8]})
      assert_equal expected, dt.to_polars
    end
  end

  def test_when_not_matched_insert
    df = Polars::DataFrame.new({"x" => [1, 2, 3], "y" => [4, 5, 6]})
    with_table(df) do |dt|
      source = Polars::DataFrame.new({"x" => [2, 3, 7], "y" => [4, 5, 8]})

      dt.merge(source, "target.x = source.x", source_alias: "source", target_alias: "target")
        .when_not_matched_insert({"x" => "source.x", "y" => "source.y"})
        .execute

      expected = Polars::DataFrame.new({"x" => [1, 2, 3, 7], "y" => [4, 5, 6, 8]})
      assert_equal expected, dt.to_polars
    end
  end
end
