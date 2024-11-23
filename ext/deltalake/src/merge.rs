use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::arrow::ffi_stream::ArrowArrayStreamReader;
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::datasource::MemTable;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::logstore::LogStoreRef;
use deltalake::operations::merge::MergeBuilder;
use deltalake::table::state::DeltaTableState;
use deltalake::{DeltaResult, DeltaTable};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::Arc;

use crate::error::RubyError;
use crate::utils::rt;
use crate::RbResult;

#[magnus::wrap(class = "DeltaLake::RbMergeBuilder")]
pub(crate) struct RbMergeBuilder {
    _builder: RefCell<Option<MergeBuilder>>,
    source_alias: Option<String>,
    target_alias: Option<String>,
    arrow_schema: Arc<ArrowSchema>,
}

impl RbMergeBuilder {
    pub fn new(
        log_store: LogStoreRef,
        snapshot: DeltaTableState,
        source: ArrowArrayStreamReader,
        predicate: String,
        source_alias: Option<String>,
        target_alias: Option<String>,
        safe_cast: bool,
    ) -> DeltaResult<Self> {
        let ctx = SessionContext::new();
        let schema = source.schema();
        let batches = vec![source.map(|batch| batch.unwrap()).collect::<Vec<_>>()];
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), batches).unwrap());
        let source_df = ctx.read_table(table_provider).unwrap();

        let mut cmd =
            MergeBuilder::new(log_store, snapshot, predicate, source_df).with_safe_cast(safe_cast);

        if let Some(src_alias) = &source_alias {
            cmd = cmd.with_source_alias(src_alias);
        }

        if let Some(trgt_alias) = &target_alias {
            cmd = cmd.with_target_alias(trgt_alias);
        }

        Ok(Self {
            _builder: RefCell::new(Some(cmd)),
            source_alias,
            target_alias,
            arrow_schema: schema,
        })
    }

    pub fn execute(&self) -> DeltaResult<(DeltaTable, String)> {
        let (table, metrics) = rt().block_on(self._builder.take().unwrap().into_future())?;
        Ok((table, serde_json::to_string(&metrics).unwrap()))
    }
}

impl RbMergeBuilder {
    pub fn when_matched_update(
        &self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
    ) -> RbResult<()> {
        let mut binding = self._builder.borrow_mut();
        *binding = match binding.take() {
            Some(cmd) => Some(
                cmd.when_matched_update(|mut update| {
                    for (column, expression) in updates {
                        update = update.update(column, expression)
                    }
                    if let Some(predicate) = predicate {
                        update = update.predicate(predicate)
                    };
                    update
                })
                .map_err(RubyError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }
}
