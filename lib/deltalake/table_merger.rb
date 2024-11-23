module DeltaLake
  class TableMerger
    def initialize(builder, table)
      @builder = builder
      @table = table
    end

    # TODO avoid updating in-place?
    def when_matched_update(updates, predicate: nil)
      @builder.when_matched_update(updates, predicate)
      self
    end

    def execute
      metrics = @table.merge_execute(@builder)
      JSON.parse(metrics)
    end
  end
end
