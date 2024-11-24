module DeltaLake
  class TableAlterer
    def initialize(table)
      @table = table
    end

    def add_feature(
      feature,
      allow_protocol_versions_increase: false
    )
      if !feature.is_a?(Array)
        feature = [feature]
      end
      @table._table.add_feature(
        feature,
        allow_protocol_versions_increase
      )
    end

    def add_columns(fields)
      if fields.is_a?(DeltaLake::Field)
        fields = [fields]
      end

      @table._table.add_columns(
        fields
      )
    end

    def add_constraint(constraints)
      if constraints.length > 1
        raise ArgumentError,
          "add_constraints is limited to a single constraint addition at once for now."
      end

      @table._table.add_constraints(
        constraints
      )
    end

    def drop_constraint(name, raise_if_not_exists: true)
      @table._table.drop_constraints(
        name,
        raise_if_not_exists
      )
    end

    def set_table_properties(
      properties,
      raise_if_not_exists: true
    )
      @table._table.set_table_properties(
        properties,
        raise_if_not_exists
      )
    end
  end
end
