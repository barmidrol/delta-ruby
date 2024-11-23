mod error;
mod schema;
mod utils;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::future::IntoFuture;
use std::str::FromStr;
use std::time;

use chrono::{DateTime, Duration, FixedOffset, Utc};
use deltalake::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use deltalake::arrow::record_batch::RecordBatchIterator;
use deltalake::datafusion::physical_plan::ExecutionPlan;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::{scalars::ScalarExt, StructType, Transaction};
use deltalake::operations::collect_sendable_stream;
use deltalake::operations::constraints::ConstraintBuilder;
use deltalake::operations::delete::DeleteBuilder;
use deltalake::operations::drop_constraints::DropConstraintBuilder;
use deltalake::operations::filesystem_check::FileSystemCheckBuilder;
use deltalake::operations::load_cdf::CdfLoadBuilder;
use deltalake::operations::optimize::{OptimizeBuilder, OptimizeType};
use deltalake::operations::restore::RestoreBuilder;
use deltalake::operations::transaction::TableReference;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::partitions::PartitionFilter;
use deltalake::storage::IORuntime;
use deltalake::DeltaOps;
use error::DeltaError;
use futures::future::join_all;

use magnus::{
    exception, function, method, prelude::*, Error, Integer, Module, RArray, RHash, Ruby, Value,
};

use crate::error::DeltaProtocolError;
use crate::error::RubyError;
use crate::schema::{schema_to_rbobject, Field};
use crate::utils::rt;

type RbResult<T> = Result<T, Error>;

#[magnus::wrap(class = "DeltaLake::RawDeltaTable")]
struct RawDeltaTable {
    _table: RefCell<deltalake::DeltaTable>,
}

#[magnus::wrap(class = "DeltaLake::RawDeltaTableMetaData")]
struct RawDeltaTableMetaData {
    id: String,
    name: Option<String>,
    description: Option<String>,
    partition_columns: Vec<String>,
    created_time: Option<i64>,
    configuration: HashMap<String, Option<String>>,
}

#[magnus::wrap(class = "DeltaLake::ArrowArrayStream")]
pub struct ArrowArrayStream {
    stream: FFI_ArrowArrayStream,
}

impl ArrowArrayStream {
    pub fn to_i(&self) -> usize {
        (&self.stream as *const _) as usize
    }
}

type StringVec = Vec<String>;

impl RawDeltaTable {
    pub fn new(
        table_uri: String,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
        log_buffer_size: Option<usize>,
    ) -> RbResult<Self> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(&table_uri)
            .with_io_runtime(IORuntime::default());

        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        if let Some(version) = version {
            builder = builder.with_version(version)
        }
        if without_files {
            builder = builder.without_files()
        }
        if let Some(buf_size) = log_buffer_size {
            builder = builder
                .with_log_buffer_size(buf_size)
                .map_err(RubyError::from)?;
        }

        let table = rt().block_on(builder.load()).map_err(RubyError::from)?;
        Ok(RawDeltaTable {
            _table: RefCell::new(table),
        })
    }

    pub fn is_deltatable(
        table_uri: String,
        storage_options: Option<HashMap<String, String>>,
    ) -> RbResult<bool> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(&table_uri);
        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        Ok(rt()
            .block_on(async {
                match builder.build() {
                    Ok(table) => table.verify_deltatable_existence().await,
                    Err(err) => Err(err),
                }
            })
            .map_err(RubyError::from)?)
    }

    pub fn table_uri(&self) -> RbResult<String> {
        Ok(self._table.borrow().table_uri())
    }

    pub fn version(&self) -> RbResult<i64> {
        Ok(self._table.borrow().version())
    }

    pub fn has_files(&self) -> RbResult<bool> {
        Ok(self._table.borrow().config.require_files)
    }

    pub fn metadata(&self) -> RbResult<RawDeltaTableMetaData> {
        let binding = self._table.borrow();
        let metadata = binding.metadata().map_err(RubyError::from)?;
        Ok(RawDeltaTableMetaData {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: metadata.description.clone(),
            partition_columns: metadata.partition_columns.clone(),
            created_time: metadata.created_time,
            configuration: metadata.configuration.clone(),
        })
    }

    pub fn protocol_versions(&self) -> RbResult<(i32, i32, Option<StringVec>, Option<StringVec>)> {
        let binding = self._table.borrow();
        let table_protocol = binding.protocol().map_err(RubyError::from)?;
        Ok((
            table_protocol.min_reader_version,
            table_protocol.min_writer_version,
            table_protocol
                .writer_features
                .as_ref()
                .and_then(|features| {
                    let empty_set = !features.is_empty();
                    empty_set.then(|| {
                        features
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                    })
                }),
            table_protocol
                .reader_features
                .as_ref()
                .and_then(|features| {
                    let empty_set = !features.is_empty();
                    empty_set.then(|| {
                        features
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                    })
                }),
        ))
    }

    pub fn load_version(&self, version: i64) -> RbResult<()> {
        Ok(rt()
            .block_on(self._table.borrow_mut().load_version(version))
            .map_err(RubyError::from)?)
    }

    pub fn get_latest_version(&self) -> RbResult<i64> {
        Ok(rt()
            .block_on(self._table.borrow().get_latest_version())
            .map_err(RubyError::from)?)
    }

    pub fn get_earliest_version(&self) -> RbResult<i64> {
        Ok(rt()
            .block_on(self._table.borrow().get_earliest_version())
            .map_err(RubyError::from)?)
    }

    pub fn get_num_index_cols(&self) -> RbResult<i32> {
        Ok(self
            ._table
            .borrow()
            .snapshot()
            .map_err(RubyError::from)?
            .config()
            .num_indexed_cols())
    }

    pub fn get_stats_columns(&self) -> RbResult<Option<Vec<String>>> {
        Ok(self
            ._table
            .borrow()
            .snapshot()
            .map_err(RubyError::from)?
            .config()
            .stats_columns()
            .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()))
    }

    pub fn load_with_datetime(&self, ds: String) -> RbResult<()> {
        let datetime = DateTime::<Utc>::from(
            DateTime::<FixedOffset>::parse_from_rfc3339(&ds).map_err(|err| {
                Error::new(
                    exception::arg_error(),
                    format!("Failed to parse datetime string: {err}"),
                )
            })?,
        );
        Ok(rt()
            .block_on(self._table.borrow_mut().load_with_datetime(datetime))
            .map_err(RubyError::from)?)
    }

    pub fn files(&self, partition_filters: Option<Value>) -> RbResult<Vec<String>> {
        if !self.has_files()? {
            return Err(DeltaError::new_err("Table is instantiated without files."));
        }

        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(RubyError::from)?;
            Ok(self
                ._table
                .borrow()
                .get_files_by_partitions(&filters)
                .map_err(RubyError::from)?
                .into_iter()
                .map(|p| p.to_string())
                .collect())
        } else {
            Ok(self
                ._table
                .borrow()
                .get_files_iter()
                .map_err(RubyError::from)?
                .map(|f| f.to_string())
                .collect())
        }
    }

    pub fn file_uris(&self, partition_filters: Option<Value>) -> RbResult<Vec<String>> {
        if !self._table.borrow().config.require_files {
            return Err(DeltaError::new_err("Table is initiated without files."));
        }

        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(RubyError::from)?;
            Ok(self
                ._table
                .borrow()
                .get_file_uris_by_partitions(&filters)
                .map_err(RubyError::from)?)
        } else {
            Ok(self
                ._table
                .borrow()
                .get_file_uris()
                .map_err(RubyError::from)?
                .collect())
        }
    }

    pub fn schema(&self) -> RbResult<Value> {
        let binding = self._table.borrow();
        let schema: &StructType = binding.get_schema().map_err(RubyError::from)?;
        schema_to_rbobject(schema.to_owned())
    }

    pub fn vacuum(
        &self,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
    ) -> RbResult<Vec<String>> {
        let mut cmd = VacuumBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        )
        .with_enforce_retention_duration(enforce_retention_duration)
        .with_dry_run(dry_run);
        if let Some(retention_period) = retention_hours {
            cmd = cmd.with_retention_period(Duration::hours(retention_period as i64));
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(metrics.files_deleted)
    }

    pub fn compact_optimize(
        &self,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        min_commit_interval: Option<u64>,
    ) -> RbResult<String> {
        let mut cmd = OptimizeBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        )
        .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get));
        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        if let Some(commit_interval) = min_commit_interval {
            cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn z_order_optimize(
        &self,
        z_order_columns: Vec<String>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        max_spill_size: usize,
        min_commit_interval: Option<u64>,
    ) -> RbResult<String> {
        let mut cmd = OptimizeBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        )
        .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get))
        .with_max_spill_size(max_spill_size)
        .with_type(OptimizeType::ZOrder(z_order_columns));
        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        if let Some(commit_interval) = min_commit_interval {
            cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn add_constraints(&self, constraints: HashMap<String, String>) -> RbResult<()> {
        let mut cmd = ConstraintBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        );

        for (col_name, expression) in constraints {
            cmd = cmd.with_constraint(col_name.clone(), expression.clone());
        }

        let table = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(())
    }

    pub fn drop_constraints(&self, name: String, raise_if_not_exists: bool) -> RbResult<()> {
        let cmd = DropConstraintBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        )
        .with_constraint(name)
        .with_raise_if_not_exists(raise_if_not_exists);

        let table = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(())
    }

    pub fn load_cdf(
        &self,
        starting_version: i64,
        ending_version: Option<i64>,
        starting_timestamp: Option<String>,
        ending_timestamp: Option<String>,
        columns: Option<Vec<String>>,
    ) -> RbResult<ArrowArrayStream> {
        let ctx = SessionContext::new();
        let mut cdf_read = CdfLoadBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        )
        .with_starting_version(starting_version);

        if let Some(ev) = ending_version {
            cdf_read = cdf_read.with_ending_version(ev);
        }
        if let Some(st) = starting_timestamp {
            let starting_ts: DateTime<Utc> = DateTime::<Utc>::from_str(&st)
                .map_err(|pe| Error::new(exception::arg_error(), pe.to_string()))?
                .to_utc();
            cdf_read = cdf_read.with_starting_timestamp(starting_ts);
        }
        if let Some(et) = ending_timestamp {
            let ending_ts = DateTime::<Utc>::from_str(&et)
                .map_err(|pe| Error::new(exception::arg_error(), pe.to_string()))?
                .to_utc();
            cdf_read = cdf_read.with_starting_timestamp(ending_ts);
        }

        if let Some(columns) = columns {
            cdf_read = cdf_read.with_columns(columns);
        }

        cdf_read = cdf_read.with_session_ctx(ctx.clone());

        let plan = rt().block_on(cdf_read.build()).map_err(RubyError::from)?;

        let mut tasks = vec![];
        for p in 0..plan.properties().output_partitioning().partition_count() {
            let inner_plan = plan.clone();
            let partition_batch = inner_plan.execute(p, ctx.task_ctx()).unwrap();
            let handle = rt().spawn(collect_sendable_stream(partition_batch));
            tasks.push(handle);
        }

        // This is unfortunate.
        let batches = rt()
            .block_on(join_all(tasks))
            .into_iter()
            .flatten()
            .collect::<Result<Vec<Vec<_>>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .map(Ok);
        let batch_iter = RecordBatchIterator::new(batches, plan.schema());
        let ffi_stream = FFI_ArrowArrayStream::new(Box::new(batch_iter));
        Ok(ArrowArrayStream { stream: ffi_stream })
    }

    pub fn restore(
        &self,
        target: Option<Value>,
        ignore_missing_files: bool,
        protocol_downgrade_allowed: bool,
    ) -> RbResult<String> {
        let mut cmd = RestoreBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        );
        if let Some(val) = target {
            if let Some(version) = Integer::from_value(val) {
                cmd = cmd.with_version_to_restore(version.to_i64()?)
            }
            if let Ok(ds) = String::try_convert(val) {
                let datetime = DateTime::<Utc>::from(
                    DateTime::<FixedOffset>::parse_from_rfc3339(ds.as_ref()).map_err(|err| {
                        Error::new(
                            exception::arg_error(),
                            format!("Failed to parse datetime string: {err}"),
                        )
                    })?,
                );
                cmd = cmd.with_datetime_to_restore(datetime)
            }
        }
        cmd = cmd.with_ignore_missing_files(ignore_missing_files);
        cmd = cmd.with_protocol_downgrade_allowed(protocol_downgrade_allowed);

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn history(&self, limit: Option<usize>) -> RbResult<Vec<String>> {
        let history = rt()
            .block_on(self._table.borrow().history(limit))
            .map_err(RubyError::from)?;
        Ok(history
            .iter()
            .map(|c| serde_json::to_string(c).unwrap())
            .collect())
    }

    pub fn update_incremental(&self) -> RbResult<()> {
        #[allow(deprecated)]
        Ok(rt()
            .block_on(self._table.borrow_mut().update_incremental(None))
            .map_err(RubyError::from)?)
    }

    fn get_active_partitions(&self) -> RbResult<RArray> {
        let binding = self._table.borrow();
        let _column_names: HashSet<&str> = binding
            .get_schema()
            .map_err(|_| DeltaProtocolError::new_err("table does not yet have a schema"))?
            .fields()
            .map(|field| field.name().as_str())
            .collect();
        let partition_columns: HashSet<&str> = binding
            .metadata()
            .map_err(RubyError::from)?
            .partition_columns
            .iter()
            .map(|col| col.as_str())
            .collect();

        let converted_filters = Vec::new();

        let partition_columns: Vec<&str> = partition_columns.into_iter().collect();

        let adds = binding
            .snapshot()
            .map_err(RubyError::from)?
            .get_active_add_actions_by_partitions(&converted_filters)
            .map_err(RubyError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RubyError::from)?;
        let active_partitions: HashSet<Vec<(&str, Option<String>)>> = adds
            .iter()
            .flat_map(|add| {
                Ok::<_, RubyError>(
                    partition_columns
                        .iter()
                        .flat_map(|col| {
                            Ok::<_, RubyError>((
                                *col,
                                add.partition_values()
                                    .map_err(RubyError::from)?
                                    .get(*col)
                                    .map(|v| v.serialize()),
                            ))
                        })
                        .collect(),
                )
            })
            .collect();

        Ok(RArray::from_iter(active_partitions))
    }

    pub fn delete(&self, predicate: Option<String>) -> RbResult<String> {
        let mut cmd = DeleteBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        );
        if let Some(predicate) = predicate {
            cmd = cmd.with_predicate(predicate);
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn repair(&self, dry_run: bool) -> RbResult<String> {
        let cmd = FileSystemCheckBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        )
        .with_dry_run(dry_run);

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn transaction_versions(&self) -> RHash {
        RHash::from_iter(
            self._table
                .borrow()
                .get_app_transaction_version()
                .into_iter()
                .map(|(app_id, transaction)| (app_id, RbTransaction::from(transaction))),
        )
    }
}

fn convert_partition_filters(
    _partitions_filters: Value,
) -> Result<Vec<PartitionFilter>, DeltaTableError> {
    todo!()
}

impl RawDeltaTableMetaData {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    fn description(&self) -> Option<String> {
        self.description.clone()
    }

    fn partition_columns(&self) -> Vec<String> {
        self.partition_columns.clone()
    }

    fn created_time(&self) -> Option<i64> {
        self.created_time
    }

    fn configuration(&self) -> HashMap<String, Option<String>> {
        self.configuration.clone()
    }
}

#[magnus::wrap(class = "DeltaLake::Transaction")]
pub struct RbTransaction {
    pub app_id: String,
    pub version: i64,
    pub last_updated: Option<i64>,
}

impl From<Transaction> for RbTransaction {
    fn from(value: Transaction) -> Self {
        RbTransaction {
            app_id: value.app_id,
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn write_to_deltalake(
    table_uri: String,
    data: Value,
    mode: String,
    table: Option<&RawDeltaTable>,
    schema_mode: Option<String>,
    partition_by: Option<Vec<String>>,
    predicate: Option<String>,
    target_file_size: Option<usize>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
) -> RbResult<()> {
    let capsule_pointer: usize = data.funcall("to_i", ())?;

    // use similar approach as Polars to avoid copy
    let stream_ptr =
        Box::new(unsafe { std::ptr::replace(capsule_pointer as _, FFI_ArrowArrayStream::empty()) });
    let stream = ArrowArrayStreamReader::try_new(*stream_ptr)
        .map_err(|err| DeltaError::new_err(err.to_string()))?;

    let batches = stream.map(|batch| batch.unwrap()).collect::<Vec<_>>();
    let save_mode = mode.parse().map_err(RubyError::from)?;

    let options = storage_options.clone().unwrap_or_default();
    let table = if let Some(table) = table {
        DeltaOps(table._table.borrow().clone())
    } else {
        rt().block_on(DeltaOps::try_from_uri_with_storage_options(
            &table_uri, options,
        ))
        .map_err(RubyError::from)?
    };

    let mut builder = table.write(batches).with_save_mode(save_mode);
    if let Some(schema_mode) = schema_mode {
        builder = builder.with_schema_mode(schema_mode.parse().map_err(RubyError::from)?);
    }
    if let Some(partition_columns) = partition_by {
        builder = builder.with_partition_columns(partition_columns);
    }

    if let Some(name) = &name {
        builder = builder.with_table_name(name);
    };

    if let Some(description) = &description {
        builder = builder.with_description(description);
    };

    if let Some(predicate) = predicate {
        builder = builder.with_replace_where(predicate);
    };

    if let Some(target_file_size) = target_file_size {
        builder = builder.with_target_file_size(target_file_size)
    };

    if let Some(config) = configuration {
        builder = builder.with_configuration(config);
    };

    rt().block_on(builder.into_future())
        .map_err(RubyError::from)?;

    Ok(())
}

#[magnus::init]
fn init(ruby: &Ruby) -> RbResult<()> {
    deltalake::aws::register_handlers(None);

    let module = ruby.define_module("DeltaLake")?;
    module.define_singleton_method("write_deltalake_rust", function!(write_to_deltalake, 12))?;

    let class = module.define_class("RawDeltaTable", ruby.class_object())?;
    class.define_singleton_method("new", function!(RawDeltaTable::new, 5))?;
    class.define_singleton_method("is_deltatable", function!(RawDeltaTable::is_deltatable, 2))?;
    class.define_method("table_uri", method!(RawDeltaTable::table_uri, 0))?;
    class.define_method("version", method!(RawDeltaTable::version, 0))?;
    class.define_method("has_files", method!(RawDeltaTable::has_files, 0))?;
    class.define_method("metadata", method!(RawDeltaTable::metadata, 0))?;
    class.define_method(
        "protocol_versions",
        method!(RawDeltaTable::protocol_versions, 0),
    )?;
    class.define_method("load_version", method!(RawDeltaTable::load_version, 1))?;
    class.define_method(
        "get_latest_version",
        method!(RawDeltaTable::get_latest_version, 0),
    )?;
    class.define_method(
        "get_earliest_version",
        method!(RawDeltaTable::get_earliest_version, 0),
    )?;
    class.define_method(
        "get_num_index_cols",
        method!(RawDeltaTable::get_num_index_cols, 0),
    )?;
    class.define_method(
        "get_stats_columns",
        method!(RawDeltaTable::get_stats_columns, 0),
    )?;
    class.define_method(
        "load_with_datetime",
        method!(RawDeltaTable::load_with_datetime, 1),
    )?;
    class.define_method("files", method!(RawDeltaTable::files, 1))?;
    class.define_method("file_uris", method!(RawDeltaTable::file_uris, 1))?;
    class.define_method("schema", method!(RawDeltaTable::schema, 0))?;
    class.define_method("vacuum", method!(RawDeltaTable::vacuum, 3))?;
    class.define_method(
        "compact_optimize",
        method!(RawDeltaTable::compact_optimize, 3),
    )?;
    class.define_method(
        "z_order_optimize",
        method!(RawDeltaTable::z_order_optimize, 5),
    )?;
    class.define_method(
        "add_constraints",
        method!(RawDeltaTable::add_constraints, 1),
    )?;
    class.define_method(
        "drop_constraints",
        method!(RawDeltaTable::drop_constraints, 2),
    )?;
    class.define_method("load_cdf", method!(RawDeltaTable::load_cdf, 5))?;
    class.define_method("restore", method!(RawDeltaTable::restore, 3))?;
    class.define_method("history", method!(RawDeltaTable::history, 1))?;
    class.define_method(
        "update_incremental",
        method!(RawDeltaTable::update_incremental, 0),
    )?;
    class.define_method(
        "get_active_partitions",
        method!(RawDeltaTable::get_active_partitions, 0),
    )?;
    class.define_method("delete", method!(RawDeltaTable::delete, 1))?;
    class.define_method("repair", method!(RawDeltaTable::repair, 1))?;
    class.define_method(
        "transaction_versions",
        method!(RawDeltaTable::transaction_versions, 0),
    )?;

    let class = module.define_class("RawDeltaTableMetaData", ruby.class_object())?;
    class.define_method("id", method!(RawDeltaTableMetaData::id, 0))?;
    class.define_method("name", method!(RawDeltaTableMetaData::name, 0))?;
    class.define_method(
        "description",
        method!(RawDeltaTableMetaData::description, 0),
    )?;
    class.define_method(
        "partition_columns",
        method!(RawDeltaTableMetaData::partition_columns, 0),
    )?;
    class.define_method(
        "created_time",
        method!(RawDeltaTableMetaData::created_time, 0),
    )?;
    class.define_method(
        "configuration",
        method!(RawDeltaTableMetaData::configuration, 0),
    )?;

    let class = module.define_class("ArrowArrayStream", ruby.class_object())?;
    class.define_method("to_i", method!(ArrowArrayStream::to_i, 0))?;

    let class = module.define_class("Field", ruby.class_object())?;
    class.define_method("name", method!(Field::name, 0))?;
    class.define_method("type", method!(Field::get_type, 0))?;
    class.define_method("nullable", method!(Field::nullable, 0))?;

    Ok(())
}
