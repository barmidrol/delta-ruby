module DeltaLake
  class TableOptimizer
    def initialize(table)
      @table = table
    end

    def compact(
      target_size: nil,
      max_concurrent_tasks: nil,
      min_commit_interval: nil
    )
      metrics =
        @table._table.compact_optimize(
          target_size,
          max_concurrent_tasks,
          min_commit_interval
        )
      @table.update_incremental
      JSON.parse(metrics)
    end
  end
end
