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

    def files
      @table.files
    end

    def file_uris
      @table.file_uris
    end

    def load_as_version(version)
      if version.is_a?(Integer)
        @table.load_version(version)
      else
        raise TypeError, "Invalid datatype provided for version, only Integer is accepted."
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
            "DELTA_DYNAMO_TABLE_NAME",
            "conditional_put"
          ]
          storage_options = @storage_options&.except(*delta_keys)
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

    # private
    def _table
      @table
    end
  end
end
