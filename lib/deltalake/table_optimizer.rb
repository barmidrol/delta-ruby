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
      result = JSON.parse(metrics)
      ["filesAdded", "filesRemoved"].each do |key|
        result[key] = JSON.parse(result[key]) if result[key].is_a?(String)
      end
      # TODO return underscore symbols like delete
      result
    end
  end
end
