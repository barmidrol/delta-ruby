use arrow_schema::ArrowError;
use deltalake::{errors::DeltaTableError, ObjectStoreError};
use magnus::{exception, Error, Module, RModule, Ruby};
use std::borrow::Cow;

macro_rules! create_exception {
    ($type:ident, $name:expr) => {
        pub struct $type {}

        impl $type {
            pub fn new_err<T>(message: T) -> Error
            where
                T: Into<Cow<'static, str>>,
            {
                let class = Ruby::get()
                    .unwrap()
                    .class_object()
                    .const_get::<_, RModule>("DeltaLake")
                    .unwrap()
                    .const_get($name)
                    .unwrap();
                Error::new(class, message)
            }
        }
    };
}

create_exception!(DeltaError, "Error");
create_exception!(TableNotFoundError, "TableNotFoundError");
create_exception!(DeltaProtocolError, "DeltaProtocolError");
create_exception!(CommitFailedError, "CommitFailedError");
create_exception!(SchemaMismatchError, "SchemaMismatchError");

fn inner_to_rb_err(err: DeltaTableError) -> Error {
    match err {
        DeltaTableError::NotATable(msg) => TableNotFoundError::new_err(msg),
        DeltaTableError::InvalidTableLocation(msg) => TableNotFoundError::new_err(msg),

        // protocol errors
        DeltaTableError::InvalidJsonLog { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidStatsJson { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidData { violations } => {
            DeltaProtocolError::new_err(format!("Invariant violations: {:?}", violations))
        }

        // commit errors
        DeltaTableError::Transaction { source } => CommitFailedError::new_err(source.to_string()),

        // ruby exceptions
        DeltaTableError::ObjectStore { source } => object_store_to_rb(source),
        DeltaTableError::Io { source } => Error::new(exception::io_error(), source.to_string()),

        DeltaTableError::Arrow { source } => arrow_to_rb(source),

        _ => DeltaError::new_err(err.to_string()),
    }
}

fn object_store_to_rb(err: ObjectStoreError) -> Error {
    match err {
        ObjectStoreError::NotFound { .. } => Error::new(exception::io_error(), err.to_string()),
        ObjectStoreError::Generic { source, .. }
            if source.to_string().contains("AWS_S3_ALLOW_UNSAFE_RENAME") =>
        {
            DeltaProtocolError::new_err(source.to_string())
        }
        _ => Error::new(exception::io_error(), err.to_string()),
    }
}

fn arrow_to_rb(err: ArrowError) -> Error {
    match err {
        ArrowError::IoError(msg, _) => Error::new(exception::io_error(), msg),
        ArrowError::DivideByZero => Error::new(exception::arg_error(), "division by zero"),
        ArrowError::InvalidArgumentError(msg) => Error::new(exception::arg_error(), msg),
        ArrowError::NotYetImplemented(msg) => Error::new(exception::not_imp_error(), msg),
        ArrowError::SchemaError(msg) => SchemaMismatchError::new_err(msg),
        other => Error::new(exception::runtime_error(), other.to_string()),
    }
}

pub enum RubyError {
    DeltaTable(DeltaTableError),
}

impl From<DeltaTableError> for RubyError {
    fn from(err: DeltaTableError) -> Self {
        RubyError::DeltaTable(err)
    }
}

impl From<RubyError> for Error {
    fn from(value: RubyError) -> Self {
        match value {
            RubyError::DeltaTable(err) => inner_to_rb_err(err),
        }
    }
}
