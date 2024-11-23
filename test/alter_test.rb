require_relative "test_helper"

class AlterTest < Minitest::Test
  def test_add_constraint
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      error = assert_raises(DeltaLake::DeltaProtocolError) do
        dt.alter.add_constraint({"a_gt_1" => "a > 1"})
      end
      assert_match "Check or Invariant (a > 1) violated", error.message

      dt.alter.add_constraint({"a_gt_0" => "a > 0"})

      df2 = Polars::DataFrame.new({"a" => [4, 5, -1]})
      error = assert_raises(DeltaLake::DeltaProtocolError) do
        DeltaLake.write(dt, df2, mode: "append")
      end
      assert_match "Check or Invariant (a > 0) violated", error.message
    end
  end

  def test_drop_constraint
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      dt.alter.add_constraint({"a_gt_0" => "a > 0"})

      dt.alter.drop_constraint("a_gt_0")

      df2 = Polars::DataFrame.new({"a" => [4, 5, -1]})
      DeltaLake.write(dt, df2, mode: "append")
    end
  end

  def test_drop_constraint_missing
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    with_table(df) do |dt|
      error = assert_raises(DeltaLake::Error) do
        dt.alter.drop_constraint("a_gt_0")
      end
      assert_equal "Generic DeltaTable error: Constraint with name: a_gt_0 doesn't exists", error.message

      dt.alter.drop_constraint("a_gt_0", raise_if_not_exists: false)
    end
  end
end
