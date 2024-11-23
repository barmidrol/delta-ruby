module DeltaLake
  class TableAlterer
    def initialize(table)
      @table = table
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
  end
end
