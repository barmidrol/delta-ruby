module DeltaLake
  class TableMerger
    def initialize(builder, table)
      @builder = builder
      @table = table
    end

    def when_matched_update(updates, predicate: nil)
      @builder.when_matched_update(updates, predicate)
      self
    end

    def when_not_matched_insert(updates, predicate: nil)
      @builder.when_not_matched_insert(updates, predicate)
      self
    end

    def when_matched_delete(predicate: nil)
      @builder.when_matched_delete(predicate)
      self
    end

    def when_not_matched_by_source_update(updates, predicate: nil)
      @builder.when_not_matched_by_source_update(updates, predicate)
      self
    end

    def when_not_matched_by_source_delete(predicate: nil)
      @builder.when_not_matched_by_source_delete(predicate)
      self
    end

    def execute
      metrics = @table.merge_execute(@builder)
      JSON.parse(metrics).transform_keys(&:to_sym)
    end
  end
end
