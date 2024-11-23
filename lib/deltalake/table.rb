module DeltaLake
  class Table
    def initialize(
      table_uri,
      version: nil,
      storage_options: nil,
      without_files: false,
      log_buffer_size: nil
    )
      @storage_options = storage_options
      @table =
        RawDeltaTable.new(
          table_uri,
          version,
          storage_options,
          without_files,
          log_buffer_size
        )
    end

    def self.exists?(table_uri, storage_options: nil)
      RawDeltaTable.is_deltatable(table_uri, storage_options)
    end

    def version
      @table.version
    end

    def partitions
      partitions = []
      @table.get_active_partitions.each do |partition|
        next unless partition
        partitions << partition.to_h
      end
      partitions
    end

    def files(partition_filters: nil)
      @table.files(_stringify_partition_values(partition_filters))
    end

    def file_uris(partition_filters: nil)
      @table.file_uris(_stringify_partition_values(partition_filters))
    end

    def load_as_version(version)
      if version.is_a?(Integer)
        @table.load_version(version)
      elsif version.is_a?(Time)
        # needed for iso8601
        require "time"

        @table.load_with_datetime(version.utc.iso8601(9))
      elsif version.is_a?(String)
        @table.load_with_datetime(version)
      else
        raise TypeError, "Invalid datatype provided for version, only Integer, String, and Time are accepted."
      end
    end

    def table_uri
      @table.table_uri
    end

    def schema
      @table.schema
    end

    def metadata
      Metadata.new(@table)
    end

    def protocol
      ProtocolVersions.new(*@table.protocol_versions)
    end

    def vacuum(
      retention_hours: nil,
      dry_run: true,
      enforce_retention_duration: true
    )
      if retention_hours
        if retention_hours < 0
          raise ArgumentError, "The retention periods should be positive."
        end
      end

      @table.vacuum(
        dry_run,
        retention_hours,
        enforce_retention_duration
      )
    end

    def optimize
      TableOptimizer.new(self)
    end

    def alter
      TableAlterer.new(self)
    end

    def to_polars(eager: true)
      require "polars-df"

      sources = file_uris
      lf =
        if sources.empty?
          Polars::LazyFrame.new
        else
          delta_keys = [
            "AWS_S3_ALLOW_UNSAFE_RENAME",
            "AWS_S3_LOCKING_PROVIDER",
            "CONDITIONAL_PUT",
            "DELTA_DYNAMO_TABLE_NAME"
          ]
          storage_options = @storage_options&.reject { |k, _| delta_keys.include?(k.to_s.upcase) }
          Polars.scan_parquet(sources, storage_options: storage_options)
        end
      eager ? lf.collect : lf
    end

    def update_incremental
      @table.update_incremental
    end

    def delete(predicate = nil)
      metrics = @table.delete(predicate)
      JSON.parse(metrics).transform_keys(&:to_sym)
    end

    def repair(dry_run: false)
      metrics =
        @table.repair(
          dry_run
        )
      JSON.parse(metrics).transform_keys(&:to_sym)
    end

    def transaction_versions
      @table.transaction_versions
    end

    # private
    def _table
      @table
    end

    # private
    def _stringify_partition_values(partition_filters)
      if partition_filters.nil?
        return partition_filters
      end

      raise "todo"
    end
  end
end
