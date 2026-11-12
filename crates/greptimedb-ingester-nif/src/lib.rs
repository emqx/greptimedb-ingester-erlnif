use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use futures::FutureExt;
use std::time::Duration;

use greptime_proto::v1::auth_header::AuthScheme;
use greptime_proto::v1::Basic;
use greptimedb_ingester::channel_manager::{ChannelConfig, ChannelManager};
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::{BulkInserter, BulkWriteOptions, ColumnDataType, TableSchema};
use rustler::{
    Atom, Encoder, Env, LocalPid, Monitor, NifResult, OwnedEnv, Resource, ResourceArc, Term,
};

use operation::{Operation, OperationResource};
use runtime::{Context, RuntimeOwner, RuntimeResource};
use rustler::env::SavedTerm;

static FIPS_ENABLED: AtomicBool = AtomicBool::new(false);

const STREAM_RESPONSE_DRAIN_INTERVAL: usize = 256;

pub mod atoms;
mod codec;
mod operation;
mod runtime;
mod types;
mod util;

pub struct GreptimeResource {
    pub db: Database,
    pub client: Client,
    channel_manager: ChannelManager,
    pub auth: Option<AuthScheme>,
    pub ts_column: Option<String>,
    pub ttl: Option<String>,
    pub stream_timeout: Duration,
    pub runtime: Arc<RuntimeOwner>,
    operation: Mutex<Option<Arc<Operation>>>,
    closed: AtomicBool,
}

pub struct StreamWriterResource {
    id: usize,
    schema: TableSchema,
    submitted_batches: AtomicUsize,
    runtime: Arc<RuntimeOwner>,
    closed: AtomicBool,
}

impl Drop for StreamWriterResource {
    fn drop(&mut self) {
        self.runtime.discard(self.id);
    }
}

impl Resource for GreptimeResource {
    const IMPLEMENTS_DOWN: bool = true;

    fn down(&self, _env: Env, _pid: LocalPid, _monitor: Monitor) {
        self.close();
    }
}

impl GreptimeResource {
    fn invalidate_channels_on(&self, error: &greptimedb_ingester::Error) {
        if matches!(
            error_kind(error),
            ErrorKind::Transport | ErrorKind::Cancelled
        ) {
            self.channel_manager.retain_channel(|_, _| false);
        }
    }

    fn lock_operation(&self) -> MutexGuard<'_, Option<Arc<Operation>>> {
        match self.operation.lock() {
            Ok(slot) => slot,
            Err(poisoned) => {
                // Stop admitting requests, but leave active work tracked until reclamation.
                self.closed.store(true, Ordering::Release);
                let slot = poisoned.into_inner();
                if let Some(operation) = slot.as_ref() {
                    operation.cancel();
                }
                self.operation.clear_poison();
                slot
            }
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let operation = self.lock_operation().clone();
        if let Some(operation) = operation {
            operation.cancel();
        }
    }

    fn drain(&self) {
        self.closed.store(true, Ordering::Release);
        let operation = self.lock_operation().clone();
        if let Some(operation) = operation {
            // Dropping a stream future can destroy its writer and older pending batches.
            if !operation.finish_stream {
                operation.cancel();
            }
        }
    }
}

impl Resource for StreamWriterResource {}

fn load(env: Env, _info: Term) -> bool {
    FIPS_ENABLED.store(check_fips_enabled(), Ordering::Relaxed);
    env.register::<GreptimeResource>().is_ok()
        && env.register::<StreamWriterResource>().is_ok()
        && env.register::<OperationResource>().is_ok()
        && env.register::<RuntimeResource>().is_ok()
}

fn check_fips_enabled() -> bool {
    std::fs::read_to_string("/proc/sys/crypto/fips_enabled")
        .map(|s| s.trim() == "1")
        .unwrap_or(false)
}

#[rustler::nif(schedule = "DirtyIo")]
fn runtime_start(env: Env, workers: usize, codec_workers: usize) -> NifResult<Term> {
    let runtime =
        runtime::start(workers, codec_workers).map_err(|e| rustler::Error::Term(Box::new(e)))?;
    let resource = ResourceArc::new(RuntimeResource(runtime));
    if resource.monitor(Some(env), &env.pid()).is_none() {
        runtime::stop(&resource);
        return Err(rustler::Error::BadArg);
    }
    Ok(resource.encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn runtime_stop(runtime: ResourceArc<RuntimeResource>, timeout_ms: u64) -> NifResult<Atom> {
    if !(1..=30_000).contains(&timeout_ms) {
        return Err(rustler::Error::BadArg);
    }
    runtime::stop_with_timeout(&runtime, Duration::from_millis(timeout_ms));
    Ok(atoms::ok())
}

#[rustler::nif]
fn fips_status() -> bool {
    FIPS_ENABLED.load(Ordering::Relaxed)
}

use greptimedb_ingester::channel_manager::{ClientTlsOption, TlsVerify};

fn decode_tls_verify(verify_term: Term) -> Result<TlsVerify, String> {
    if let Ok(verify_atom) = verify_term.decode::<Atom>() {
        if verify_atom == atoms::verify_none() {
            return Ok(TlsVerify::VerifyNone);
        }
        if verify_atom == atoms::verify_peer() {
            return Ok(TlsVerify::VerifyPeer);
        }
        if verify_atom == rustler::types::atom::undefined()
            || verify_atom == rustler::types::atom::nil()
        {
            return Ok(TlsVerify::VerifyPeer);
        }
    }

    if let Ok(verify_str) = verify_term.decode::<String>() {
        return match verify_str.as_str() {
            "verify_none" => Ok(TlsVerify::VerifyNone),
            "verify_peer" => Ok(TlsVerify::VerifyPeer),
            "undefined" | "nil" => Ok(TlsVerify::VerifyPeer),
            _ => Err(format!("invalid verify option `{verify_str}`")),
        };
    }

    Err("invalid verify option".to_string())
}

#[rustler::nif(schedule = "DirtyIo")]
fn connect(opts: Term) -> NifResult<Term> {
    let env = opts.get_env();

    let runtime = runtime::shared().map_err(|e| rustler::Error::Term(Box::new(e)))?;

    let endpoints_term: Term = opts.map_get(atoms::endpoints().to_term(env))?;
    let dbname_term: Term = opts.map_get(atoms::dbname().to_term(env))?;

    let endpoints: Vec<String> = endpoints_term.decode()?;
    let dbname: String = dbname_term.decode()?;

    // The operation deadline includes queueing and response consumption.
    let channel_config = ChannelConfig {
        timeout: None,
        ..Default::default()
    };
    let (client, channel_manager) = if let Ok(tls_term) = opts.map_get(atoms::tls().to_term(env)) {
        if tls_term.decode::<bool>()? {
            let tls_verify = match opts.map_get(atoms::verify().to_term(env)) {
                Ok(verify_term) => match decode_tls_verify(verify_term) {
                    Ok(verify) => verify,
                    Err(err) => {
                        return Ok(Failure::Native(NativeFailure::local(
                            ErrorKind::Configuration,
                            ErrorStage::Connect,
                            WriteOutcome::NotSent,
                            err,
                        ))
                        .encode(env))
                    }
                },
                Err(_) => TlsVerify::VerifyPeer,
            };
            let server_ca_cert_path = opts
                .map_get(atoms::ca_cert().to_term(env))
                .and_then(|t| t.decode())
                .unwrap_or_default();
            let client_cert_path = opts
                .map_get(atoms::client_cert().to_term(env))
                .and_then(|t| t.decode())
                .unwrap_or_default();
            let client_key_path = opts
                .map_get(atoms::client_key().to_term(env))
                .and_then(|t| t.decode())
                .unwrap_or_default();

            let tls_option = ClientTlsOption {
                server_ca_cert_path,
                client_cert_path,
                client_key_path,
                cipher_suites: opts
                    .map_get(atoms::cipher_suites().to_term(env))
                    .and_then(|t| t.decode())
                    .unwrap_or_default(),
            };

            match ChannelManager::with_tls_config(
                channel_config
                    .tls_verify(tls_verify)
                    .client_tls_config(tls_option),
            ) {
                Ok(manager) => (
                    Client::with_manager_and_urls(manager.clone(), endpoints),
                    manager,
                ),
                Err(error) => {
                    return Ok(
                        Failure::native(error, ErrorStage::Connect, WriteOutcome::NotSent)
                            .encode(env),
                    )
                }
            }
        } else {
            let manager = ChannelManager::with_config(channel_config);
            (
                Client::with_manager_and_urls(manager.clone(), endpoints),
                manager,
            )
        }
    } else {
        let manager = ChannelManager::with_config(channel_config);
        (
            Client::with_manager_and_urls(manager.clone(), endpoints),
            manager,
        )
    };

    let mut db = Database::new_with_dbname(dbname, client.clone());
    let mut auth = None;
    let ts_column = opts
        .map_get(atoms::ts_column().to_term(env))
        .ok()
        .and_then(|term| term.decode().ok());
    let ttl = opts
        .map_get(atoms::ttl().to_term(env))
        .ok()
        .and_then(|term| term.decode().ok());
    let stream_timeout_ms = match opts.map_get(atoms::stream_timeout().to_term(env)) {
        Ok(timeout) => timeout.decode::<u64>()?,
        Err(_) => 30_000,
    };
    if stream_timeout_ms == 0 {
        return Err(rustler::Error::BadArg);
    }
    let stream_timeout = Duration::from_millis(stream_timeout_ms);

    if let Ok(username_term) = opts.map_get(atoms::username().to_term(env)) {
        if let Ok(password_term) = opts.map_get(atoms::password().to_term(env)) {
            let username: String = username_term.decode()?;
            let password: String = password_term.decode()?;
            let auth_scheme = AuthScheme::Basic(Basic { username, password });
            db.set_auth(auth_scheme.clone());
            auth = Some(auth_scheme);
        }
    }

    let resource = ResourceArc::new(GreptimeResource {
        db,
        client,
        channel_manager,
        auth,
        ts_column,
        ttl,
        stream_timeout,
        runtime,
        operation: Mutex::new(None),
        closed: AtomicBool::new(false),
    });
    resource
        .monitor(Some(env), &env.pid())
        .ok_or(rustler::Error::BadArg)?;
    Ok((atoms::ok(), resource).encode(env))
}

#[rustler::nif]
fn disconnect(resource: ResourceArc<GreptimeResource>) -> Atom {
    resource.close();
    atoms::ok()
}

#[rustler::nif]
fn drain(resource: ResourceArc<GreptimeResource>) -> Atom {
    resource.drain();
    atoms::ok()
}

enum TableSchemaLookup {
    Found(TableSchema),
    NotFound,
    UnsupportedColumnType(String),
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn describe_table_sql(table_name: &str) -> String {
    format!("DESCRIBE {}", quote_sql_identifier(table_name))
}

async fn describe_table(
    client: &GreptimeResource,
    table_name: &str,
) -> Result<TableSchemaLookup, Failure> {
    let sql = describe_table_sql(table_name);
    let mut stream = match client.db.query(&sql).await {
        Ok(stream) => stream,
        Err(error) if is_table_not_found_error(&error) => return Ok(TableSchemaLookup::NotFound),
        Err(error) => {
            client.invalidate_channels_on(&error);
            return Err(Failure::native(
                error,
                ErrorStage::Metadata,
                WriteOutcome::NotSent,
            ));
        }
    };

    let mut table_schema = TableSchema::builder()
        .name(table_name)
        .build()
        .map_err(|e| Failure::message(e.to_string()))?;
    let mut seen_columns = false;

    while let Some(batch_res) = futures::StreamExt::next(&mut stream).await {
        let batch = match batch_res {
            Ok(batch) => batch,
            Err(error) if is_table_not_found_error(&error) => {
                return Ok(TableSchemaLookup::NotFound)
            }
            Err(error) => {
                client.invalidate_channels_on(&error);
                return Err(Failure::native(
                    error,
                    ErrorStage::Metadata,
                    WriteOutcome::NotSent,
                ));
            }
        };

        // Ensure columns exist
        // 0: Field, 1: Type, 2: Null, 3: Key, 4: Default, 5: Semantic Type
        if batch.num_columns() < 6 {
            return Err(Failure::message(
                "DESCRIBE result has unexpected number of columns",
            ));
        }

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| Failure::message("Failed to cast column 0 to StringArray"))?;
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| Failure::message("Failed to cast column 1 to StringArray"))?;
        let semantic_types = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| Failure::message("Failed to cast column 5 to StringArray"))?;

        for i in 0..batch.num_rows() {
            seen_columns = true;
            let name = names.value(i);
            let type_str = types.value(i);
            let semantic_str = semantic_types.value(i);

            let dtype = match type_str {
                "Int8" => ColumnDataType::Int8,
                "Int16" => ColumnDataType::Int16,
                "Int32" => ColumnDataType::Int32,
                "Int64" => ColumnDataType::Int64,
                "UInt8" => ColumnDataType::Uint8,
                "UInt16" => ColumnDataType::Uint16,
                "UInt32" => ColumnDataType::Uint32,
                "UInt64" => ColumnDataType::Uint64,
                "Float32" => ColumnDataType::Float32,
                "Float64" => ColumnDataType::Float64,
                "String" => ColumnDataType::String,
                "Boolean" => ColumnDataType::Boolean,
                "Binary" => ColumnDataType::Binary,
                "Date" => ColumnDataType::Date,
                "Datetime" => ColumnDataType::Datetime,
                "TimestampSecond" => ColumnDataType::TimestampSecond,
                "TimestampMillisecond" => ColumnDataType::TimestampMillisecond,
                "TimestampMicrosecond" => ColumnDataType::TimestampMicrosecond,
                "TimestampNanosecond" => ColumnDataType::TimestampNanosecond,
                _ => {
                    return Ok(TableSchemaLookup::UnsupportedColumnType(
                        type_str.to_string(),
                    ))
                }
            };

            match semantic_str {
                "TAG" => table_schema = table_schema.add_tag(name, dtype),
                "FIELD" => table_schema = table_schema.add_field(name, dtype),
                "TIMESTAMP" => table_schema = table_schema.add_timestamp(name, dtype),
                _ => {}
            }
        }
    }

    if seen_columns {
        Ok(TableSchemaLookup::Found(table_schema))
    } else {
        Ok(TableSchemaLookup::NotFound)
    }
}

struct Input {
    env: OwnedEnv,
    rows: SavedTerm,
    small: bool,
}

impl Input {
    fn new(rows: Term, small: bool) -> Self {
        let env = OwnedEnv::new();
        // Saving terms copies them synchronously before this NIF returns.
        // Large inputs can delay the connection process's deadline handling;
        // cancellation cannot interrupt this copy. Callers should bound batch size.
        let rows = env.save(rows);
        Self { env, rows, small }
    }

    fn is_empty(&self) -> bool {
        self.env.run(|env| self.rows.load(env).is_empty_list())
    }

    fn convert<T>(&self, f: impl FnOnce(Vec<Term>) -> NifResult<T>) -> Result<T, Failure> {
        self.env.run(|env| {
            let rows = self
                .rows
                .load(env)
                .decode()
                .map_err(|e| Failure::from_nif(env, e))?;
            f(rows).map_err(|e| Failure::from_nif(env, e))
        })
    }
}

enum Request {
    Execute(String),
    Insert(String, Input, ColumnDataType),
    StreamStart(String),
    StreamWrite(ResourceArc<StreamWriterResource>, Input),
    StreamClose(ResourceArc<StreamWriterResource>),
    StreamCloseAll(Vec<ResourceArc<StreamWriterResource>>),
}

impl Request {
    fn decode(command: Atom, args: Term, small: bool) -> NifResult<Self> {
        let args: Vec<Term> = args.decode()?;
        match (command, args.as_slice()) {
            (c, [sql]) if c == atoms::execute() => Ok(Self::Execute(sql.decode()?)),
            (c, [table, rows]) if c == atoms::insert() => Ok(Self::Insert(
                table.decode()?,
                Input::new(*rows, small),
                ColumnDataType::TimestampMillisecond,
            )),
            (c, [table, rows, precision]) if c == atoms::insert() => Ok(Self::Insert(
                table.decode()?,
                Input::new(*rows, small),
                timestamp_data_type(*precision)?,
            )),
            (c, [table, _]) if c == atoms::stream_start() => Ok(Self::StreamStart(table.decode()?)),
            (c, [writer, rows]) if c == atoms::stream_write() => Ok(Self::StreamWrite(
                writer.decode()?,
                Input::new(*rows, small),
            )),
            (c, [writer]) if c == atoms::stream_close() => Ok(Self::StreamClose(writer.decode()?)),
            (c, [writers]) if c == atoms::stream_close_all() => {
                Ok(Self::StreamCloseAll(writers.decode()?))
            }
            _ => Err(rustler::Error::BadArg),
        }
    }

    fn writer(&self) -> Option<ResourceArc<StreamWriterResource>> {
        match self {
            Self::StreamWrite(writer, _) | Self::StreamClose(writer) => Some(writer.clone()),
            _ => None,
        }
    }
}

fn timestamp_data_type(term: Term) -> NifResult<ColumnDataType> {
    let precision = term.decode::<Atom>()?;
    if precision == atoms::s() {
        Ok(ColumnDataType::TimestampSecond)
    } else if precision == atoms::ms() {
        Ok(ColumnDataType::TimestampMillisecond)
    } else if precision == atoms::us() {
        Ok(ColumnDataType::TimestampMicrosecond)
    } else if precision == atoms::ns() {
        Ok(ColumnDataType::TimestampNanosecond)
    } else {
        Err(rustler::Error::BadArg)
    }
}

enum Value {
    Ok,
    Count(u32),
    Batches(Vec<arrow::record_batch::RecordBatch>),
    Writer(ResourceArc<StreamWriterResource>),
}

impl Value {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Self::Ok => atoms::ok().encode(env),
            Self::Count(count) => (atoms::ok(), count).encode(env),
            Self::Writer(writer) => (atoms::ok(), writer).encode(env),
            Self::Batches(batches) => {
                let rows: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| types::record_batch_to_terms(env, batch))
                    .collect();
                (atoms::ok(), rows).encode(env)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorKind {
    Transport,
    Server,
    InvalidArgument,
    Configuration,
    Protocol,
    Internal,
    Timeout,
    Cancelled,
    Closed,
}

impl ErrorKind {
    fn atom(self) -> Atom {
        match self {
            Self::Transport => atoms::transport(),
            Self::Server => atoms::server(),
            Self::InvalidArgument => atoms::invalid_argument(),
            Self::Configuration => atoms::configuration(),
            Self::Protocol => atoms::protocol(),
            Self::Internal => atoms::internal(),
            Self::Timeout => atoms::timeout(),
            Self::Cancelled => atoms::cancelled(),
            Self::Closed => atoms::closed(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorSource {
    Server,
    CreateChannel,
    RequestTimeout,
    StreamEnded,
    StreamEndedWithPendingRequests,
    SendData,
    InvalidTlsConfig,
    InvalidConfigFilePath,
    IllegalGrpcClientState,
    MissingField,
    IllegalDatabaseResponse,
    InvalidTonicMetadataValue,
    SerdeJson,
    CreateRecordBatch,
    UnsupportedDataType,
    SerializeMetadata,
    SchemaMismatch,
    InvalidColumnCount,
    InvalidColumnIndex,
    EmptyRows,
    AddRowToBuiltBatch,
    UnflushedRows,
    UnexpectedFlightMessage,
    Local,
    TableNotFound,
}

impl ErrorSource {
    fn atom(self) -> Atom {
        match self {
            Self::Server => atoms::server(),
            Self::CreateChannel => atoms::create_channel(),
            Self::RequestTimeout => atoms::request_timeout(),
            Self::StreamEnded => atoms::stream_ended(),
            Self::StreamEndedWithPendingRequests => atoms::stream_ended_with_pending_requests(),
            Self::SendData => atoms::send_data(),
            Self::InvalidTlsConfig => atoms::invalid_tls_config(),
            Self::InvalidConfigFilePath => atoms::invalid_config_file_path(),
            Self::IllegalGrpcClientState => atoms::illegal_grpc_client_state(),
            Self::MissingField => atoms::missing_field(),
            Self::IllegalDatabaseResponse => atoms::illegal_database_response(),
            Self::InvalidTonicMetadataValue => atoms::invalid_tonic_metadata_value(),
            Self::SerdeJson => atoms::serde_json(),
            Self::CreateRecordBatch => atoms::create_record_batch(),
            Self::UnsupportedDataType => atoms::unsupported_data_type(),
            Self::SerializeMetadata => atoms::serialize_metadata(),
            Self::SchemaMismatch => atoms::schema_mismatch(),
            Self::InvalidColumnCount => atoms::invalid_column_count(),
            Self::InvalidColumnIndex => atoms::invalid_column_index(),
            Self::EmptyRows => atoms::empty_rows(),
            Self::AddRowToBuiltBatch => atoms::add_row_to_built_batch(),
            Self::UnflushedRows => atoms::unflushed_rows(),
            Self::UnexpectedFlightMessage => atoms::unexpected_flight_message(),
            Self::Local => atoms::local(),
            Self::TableNotFound => atoms::table_not_found(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorCode {
    None,
    Unknown,
    Cancelled,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

impl From<tonic::Code> for ErrorCode {
    fn from(code: tonic::Code) -> Self {
        match code {
            tonic::Code::Ok => Self::None,
            tonic::Code::Cancelled => Self::Cancelled,
            tonic::Code::Unknown => Self::Unknown,
            tonic::Code::InvalidArgument => Self::InvalidArgument,
            tonic::Code::DeadlineExceeded => Self::DeadlineExceeded,
            tonic::Code::NotFound => Self::NotFound,
            tonic::Code::AlreadyExists => Self::AlreadyExists,
            tonic::Code::PermissionDenied => Self::PermissionDenied,
            tonic::Code::ResourceExhausted => Self::ResourceExhausted,
            tonic::Code::FailedPrecondition => Self::FailedPrecondition,
            tonic::Code::Aborted => Self::Aborted,
            tonic::Code::OutOfRange => Self::OutOfRange,
            tonic::Code::Unimplemented => Self::Unimplemented,
            tonic::Code::Internal => Self::Internal,
            tonic::Code::Unavailable => Self::Unavailable,
            tonic::Code::DataLoss => Self::DataLoss,
            tonic::Code::Unauthenticated => Self::Unauthenticated,
        }
    }
}

impl ErrorCode {
    fn atom(self) -> Atom {
        match self {
            Self::None => atoms::none(),
            Self::Unknown => atoms::unknown(),
            Self::Cancelled => atoms::cancelled(),
            Self::InvalidArgument => atoms::invalid_argument(),
            Self::DeadlineExceeded => atoms::deadline_exceeded(),
            Self::NotFound => atoms::not_found(),
            Self::AlreadyExists => atoms::already_exists(),
            Self::PermissionDenied => atoms::permission_denied(),
            Self::ResourceExhausted => atoms::resource_exhausted(),
            Self::FailedPrecondition => atoms::failed_precondition(),
            Self::Aborted => atoms::aborted(),
            Self::OutOfRange => atoms::out_of_range(),
            Self::Unimplemented => atoms::unimplemented(),
            Self::Internal => atoms::internal(),
            Self::Unavailable => atoms::unavailable(),
            Self::DataLoss => atoms::data_loss(),
            Self::Unauthenticated => atoms::unauthenticated(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorStage {
    Connect,
    Metadata,
    Query,
    Insert,
    StreamStart,
    StreamWrite,
    StreamClose,
}

impl ErrorStage {
    fn atom(self) -> Atom {
        match self {
            Self::Connect => atoms::connect(),
            Self::Metadata => atoms::metadata(),
            Self::Query => atoms::query(),
            Self::Insert => atoms::insert(),
            Self::StreamStart => atoms::stream_start(),
            Self::StreamWrite => atoms::stream_write(),
            Self::StreamClose => atoms::stream_close(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteOutcome {
    NotSent,
    Unknown,
}

impl WriteOutcome {
    fn atom(self) -> Atom {
        match self {
            Self::NotSent => atoms::not_sent(),
            Self::Unknown => atoms::unknown(),
        }
    }
}

#[derive(Debug, Clone)]
struct NativeFailure {
    kind: ErrorKind,
    source: ErrorSource,
    code: ErrorCode,
    stage: ErrorStage,
    write_outcome: WriteOutcome,
    retry_hint: bool,
    message: String,
}

impl NativeFailure {
    fn from_greptimedb(
        error: greptimedb_ingester::Error,
        stage: ErrorStage,
        write_outcome: WriteOutcome,
    ) -> Self {
        let source = error_source(&error);
        let code = match &error {
            greptimedb_ingester::Error::Server { status, .. } => status.code().into(),
            greptimedb_ingester::Error::RequestTimeout { .. } => ErrorCode::DeadlineExceeded,
            _ => ErrorCode::None,
        };
        Self {
            kind: error_kind(&error),
            source,
            code,
            stage,
            write_outcome,
            retry_hint: error.is_retriable(),
            message: error.to_string(),
        }
    }

    fn table_not_found(stage: ErrorStage) -> Self {
        Self {
            kind: ErrorKind::Server,
            source: ErrorSource::TableNotFound,
            code: ErrorCode::NotFound,
            stage,
            write_outcome: WriteOutcome::NotSent,
            retry_hint: false,
            message: "table not found".to_string(),
        }
    }

    fn local(
        kind: ErrorKind,
        stage: ErrorStage,
        write_outcome: WriteOutcome,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            source: ErrorSource::Local,
            code: ErrorCode::None,
            stage,
            write_outcome,
            retry_hint: false,
            message: message.into(),
        }
    }

    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let keys = vec![
            atoms::domain().encode(env),
            atoms::kind().encode(env),
            atoms::source().encode(env),
            atoms::code().encode(env),
            atoms::stage().encode(env),
            atoms::write_outcome().encode(env),
            atoms::retry_hint().encode(env),
            atoms::message().encode(env),
        ];
        let values = vec![
            atoms::greptimedb().encode(env),
            self.kind.atom().encode(env),
            self.source.atom().encode(env),
            self.code.atom().encode(env),
            self.stage.atom().encode(env),
            self.write_outcome.atom().encode(env),
            self.retry_hint.encode(env),
            self.message.encode(env),
        ];
        let details =
            Term::map_from_term_arrays(env, &keys, &values).expect("fixed native error map");
        (atoms::greptimedb_error(), details).encode(env)
    }
}

fn error_source(error: &greptimedb_ingester::Error) -> ErrorSource {
    use greptimedb_ingester::Error;
    match error {
        Error::InvalidTlsConfig { .. } => ErrorSource::InvalidTlsConfig,
        Error::InvalidConfigFilePath { .. } => ErrorSource::InvalidConfigFilePath,
        Error::CreateChannel { .. } => ErrorSource::CreateChannel,
        Error::IllegalGrpcClientState { .. } => ErrorSource::IllegalGrpcClientState,
        Error::MissingField { .. } => ErrorSource::MissingField,
        Error::Server { .. } => ErrorSource::Server,
        Error::IllegalDatabaseResponse { .. } => ErrorSource::IllegalDatabaseResponse,
        Error::InvalidTonicMetadataValue { .. } => ErrorSource::InvalidTonicMetadataValue,
        Error::SerdeJson { .. } => ErrorSource::SerdeJson,
        Error::CreateRecordBatch { .. } => ErrorSource::CreateRecordBatch,
        Error::UnsupportedDataType { .. } => ErrorSource::UnsupportedDataType,
        Error::SerializeMetadata { .. } => ErrorSource::SerializeMetadata,
        Error::SendData { .. } => ErrorSource::SendData,
        Error::StreamEnded { .. } => ErrorSource::StreamEnded,
        Error::StreamEndedWithPendingRequests { .. } => ErrorSource::StreamEndedWithPendingRequests,
        Error::RequestTimeout { .. } => ErrorSource::RequestTimeout,
        Error::SchemaMismatch { .. } => ErrorSource::SchemaMismatch,
        Error::InvalidColumnCount { .. } => ErrorSource::InvalidColumnCount,
        Error::InvalidColumnIndex { .. } => ErrorSource::InvalidColumnIndex,
        Error::EmptyRows { .. } => ErrorSource::EmptyRows,
        Error::AddRowToBuiltBatch { .. } => ErrorSource::AddRowToBuiltBatch,
        Error::UnflushedRows { .. } => ErrorSource::UnflushedRows,
        Error::UnexpectedFlightMessage { .. } => ErrorSource::UnexpectedFlightMessage,
    }
}

fn error_kind(error: &greptimedb_ingester::Error) -> ErrorKind {
    use greptimedb_ingester::Error;
    match error {
        Error::Server { status, .. } if is_transport_status(status) => ErrorKind::Transport,
        Error::Server { status, .. } if status.code() == tonic::Code::Cancelled => {
            ErrorKind::Cancelled
        }
        Error::Server { .. } => ErrorKind::Server,
        Error::CreateChannel { .. }
        | Error::SendData { .. }
        | Error::StreamEnded { .. }
        | Error::StreamEndedWithPendingRequests { .. } => ErrorKind::Transport,
        Error::RequestTimeout { .. } => ErrorKind::Timeout,
        Error::InvalidTlsConfig { .. } | Error::InvalidConfigFilePath { .. } => {
            ErrorKind::Configuration
        }
        Error::IllegalDatabaseResponse { .. }
        | Error::SerdeJson { .. }
        | Error::UnexpectedFlightMessage { .. } => ErrorKind::Protocol,
        Error::IllegalGrpcClientState { .. }
        | Error::MissingField { .. }
        | Error::SerializeMetadata { .. } => ErrorKind::Internal,
        Error::InvalidTonicMetadataValue { .. }
        | Error::CreateRecordBatch { .. }
        | Error::UnsupportedDataType { .. }
        | Error::SchemaMismatch { .. }
        | Error::InvalidColumnCount { .. }
        | Error::InvalidColumnIndex { .. }
        | Error::EmptyRows { .. }
        | Error::AddRowToBuiltBatch { .. }
        | Error::UnflushedRows { .. } => ErrorKind::InvalidArgument,
    }
}

fn is_transport_status(status: &tonic::Status) -> bool {
    is_transport_message(status.message())
        || matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
        )
}

fn is_transport_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "transport error",
        "h2 protocol error",
        "connection error",
        "error reading a body from connection",
        "connection reset by peer",
        "connection closed",
        "broken pipe",
        "unexpected eof",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn write_outcome_for_sdk_error(error: &greptimedb_ingester::Error) -> WriteOutcome {
    use greptimedb_ingester::Error;
    match error {
        Error::InvalidTlsConfig { .. }
        | Error::InvalidConfigFilePath { .. }
        | Error::CreateChannel { .. }
        | Error::IllegalGrpcClientState { .. }
        | Error::MissingField { .. }
        | Error::InvalidTonicMetadataValue { .. }
        | Error::CreateRecordBatch { .. }
        | Error::UnsupportedDataType { .. }
        | Error::SerializeMetadata { .. }
        | Error::SchemaMismatch { .. }
        | Error::InvalidColumnCount { .. }
        | Error::InvalidColumnIndex { .. }
        | Error::EmptyRows { .. }
        | Error::AddRowToBuiltBatch { .. }
        | Error::UnflushedRows { .. } => WriteOutcome::NotSent,
        Error::Server { .. }
        | Error::IllegalDatabaseResponse { .. }
        | Error::SerdeJson { .. }
        | Error::SendData { .. }
        | Error::StreamEnded { .. }
        | Error::StreamEndedWithPendingRequests { .. }
        | Error::RequestTimeout { .. }
        | Error::UnexpectedFlightMessage { .. } => WriteOutcome::Unknown,
    }
}

enum Failure {
    Native(NativeFailure),
    Message(String),
    Raised { env: OwnedEnv, reason: SavedTerm },
    BadArg,
    Timeout,
    Cancelled,
    Closed,
    Panic,
}

impl Failure {
    fn native(
        error: greptimedb_ingester::Error,
        stage: ErrorStage,
        write_outcome: WriteOutcome,
    ) -> Self {
        Self::Native(NativeFailure::from_greptimedb(error, stage, write_outcome))
    }

    fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    fn from_nif(env: Env, error: rustler::Error) -> Self {
        match error {
            rustler::Error::Term(reason) => Self::Message(
                reason
                    .encode(env)
                    .decode()
                    .unwrap_or_else(|_| "invalid input".into()),
            ),
            rustler::Error::RaiseTerm(reason) => {
                let saved_env = OwnedEnv::new();
                let reason = saved_env.save(reason.encode(env));
                Self::Raised {
                    env: saved_env,
                    reason,
                }
            }
            _ => Self::BadArg,
        }
    }

    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Self::Native(reason) => (atoms::error(), reason.encode(env)).encode(env),
            Self::Message(reason) => (atoms::error(), reason).encode(env),
            Self::Raised {
                env: source,
                reason,
            } => {
                let reason = source.run(|source_env| reason.load(source_env).in_env(env));
                (atoms::error(), (atoms::nif_error(), reason)).encode(env)
            }
            Self::BadArg => (
                atoms::error(),
                (atoms::nif_error(), rustler::types::atom::badarg()),
            )
                .encode(env),
            Self::Timeout => (atoms::error(), atoms::timeout()).encode(env),
            Self::Closed => (atoms::error(), atoms::closed()).encode(env),
            Self::Cancelled => (atoms::error(), atoms::cancelled()).encode(env),
            Self::Panic => (atoms::error(), (atoms::nif_error(), atoms::task_panic())).encode(env),
        }
    }
}

fn is_table_not_found_error(error: &greptimedb_ingester::Error) -> bool {
    matches!(
        error,
        greptimedb_ingester::Error::Server { status, .. }
            if status.code() == tonic::Code::NotFound
                && greptimedb_error_code(status) == Some("4001")
    )
}

fn greptimedb_error_code(status: &tonic::Status) -> Option<&str> {
    status
        .metadata()
        .get("x-greptime-err-code")
        .and_then(|value| value.to_str().ok())
}

struct Running {
    client: ResourceArc<GreptimeResource>,
    operation: Arc<Operation>,
}

impl Drop for Running {
    fn drop(&mut self) {
        let mut slot = self.client.lock_operation();
        if slot
            .as_ref()
            .is_some_and(|operation| Arc::ptr_eq(operation, &self.operation))
        {
            *slot = None;
        }
        self.operation.finish();
    }
}

#[rustler::nif]
fn bounded_submission<'a>(env: Env<'a>, arguments: Term<'a>) -> Term<'a> {
    let started = std::time::Instant::now();
    let mut binaries = Vec::new();
    let bounded = bounded_arguments(arguments, &mut binaries);
    consume_submission_timeslice(env, started);
    if bounded {
        binaries.encode(env)
    } else {
        false.encode(env)
    }
}

fn bounded_arguments<'a>(arguments: Term<'a>, binaries: &mut Vec<Term<'a>>) -> bool {
    let Ok((first, rest)) = arguments.list_get_cell() else {
        return false;
    };
    if first.is_binary() {
        binaries.push(first);
    } else if !first.is_ref() {
        return false;
    }
    if rest.is_empty_list() {
        return true;
    }
    let Ok((mut rows, tail)) = rest.list_get_cell() else {
        return false;
    };
    if !tail.is_empty_list() {
        let Ok((precision, after_precision)) = tail.list_get_cell() else {
            return false;
        };
        if precision.get_type() != rustler::TermType::Atom || !after_precision.is_empty_list() {
            return false;
        }
    }
    let mut cursor = rows;
    for _ in 0..128 {
        if cursor.is_empty_list() {
            break;
        }
        let Ok((_, tail)) = cursor.list_get_cell() else {
            return false;
        };
        cursor = tail;
    }
    if !cursor.is_empty_list() {
        return false;
    }
    let mut budget = 2048;
    for _ in 0..128 {
        if rows.is_empty_list() {
            return true;
        }
        let Ok((row, tail)) = rows.list_get_cell() else {
            return false;
        };
        if !bounded_value(row, 3, &mut budget, binaries) {
            return false;
        }
        rows = tail;
    }
    rows.is_empty_list()
}

fn bounded_value<'a>(
    term: Term<'a>,
    depth: usize,
    budget: &mut usize,
    binaries: &mut Vec<Term<'a>>,
) -> bool {
    if *budget == 0 {
        return false;
    }
    *budget -= 1;
    match term.get_type() {
        rustler::TermType::Binary => {
            binaries.push(term);
            true
        }
        rustler::TermType::Atom | rustler::TermType::Float => true,
        rustler::TermType::Integer => term.decode::<i64>().is_ok() || term.decode::<u64>().is_ok(),
        rustler::TermType::Map if depth > 0 && term.map_size().is_ok_and(|size| size <= 8) => {
            let Ok(entries) = term.decode::<rustler::MapIterator>() else {
                return false;
            };
            entries.into_iter().all(|(key, value)| {
                bounded_value(key, depth - 1, budget, binaries)
                    && bounded_value(value, depth - 1, budget, binaries)
            })
        }
        _ => false,
    }
}

#[rustler::nif]
fn submit_small<'a>(
    env: Env<'a>,
    client: ResourceArc<GreptimeResource>,
    command: Atom,
    arguments: Term<'a>,
    token: Term<'a>,
    deadline_us: i64,
) -> NifResult<Term<'a>> {
    let started = std::time::Instant::now();
    let result = submit_impl(env, client, command, arguments, token, deadline_us, true);
    consume_submission_timeslice(env, started);
    result
}

fn consume_submission_timeslice(env: Env, started: std::time::Instant) {
    // Account normal NIF work against an estimated 1 ms scheduler timeslice.
    let percent = started.elapsed().as_micros().div_ceil(10).clamp(1, 100) as i32;
    rustler::schedule::consume_timeslice(env, percent);
}

#[rustler::nif(schedule = "DirtyCpu")]
fn submit_dirty<'a>(
    env: Env<'a>,
    client: ResourceArc<GreptimeResource>,
    command: Atom,
    arguments: Term<'a>,
    token: Term<'a>,
    deadline_us: i64,
) -> NifResult<Term<'a>> {
    submit_impl(env, client, command, arguments, token, deadline_us, false)
}

fn submit_impl<'a>(
    env: Env<'a>,
    client: ResourceArc<GreptimeResource>,
    command: Atom,
    arguments: Term<'a>,
    token: Term<'a>,
    deadline_us: i64,
    small: bool,
) -> NifResult<Term<'a>> {
    let now =
        unsafe { rustler::sys::enif_monotonic_time(rustler::sys::ErlNifTimeUnit::ERL_NIF_USEC) };
    let remaining = deadline_us.saturating_sub(now).max(0) as u64;
    let deadline = tokio::time::Instant::now() + Duration::from_micros(remaining);
    let request = Request::decode(command, arguments, small)?;
    let closing = matches!(&request, Request::StreamCloseAll(_));
    let writer = request.writer();
    let finish_stream = writer.is_some() || closing;
    let (operation, cancelled) = Operation::new(finish_stream);
    let running = Running {
        client: client.clone(),
        operation: operation.clone(),
    };
    let operation_resource = ResourceArc::new(OperationResource(operation.clone()));
    operation_resource
        .monitor(Some(env), &env.pid())
        .ok_or(rustler::Error::BadArg)?;
    let rejected = {
        let mut slot = client.lock_operation();
        // A draining connection only admits the final writer flush.
        if (client.closed.load(Ordering::Acquire) && !closing) || client.runtime.is_stopping() {
            Some(atoms::closed())
        } else if slot.is_some() {
            Some(atoms::busy())
        } else {
            *slot = Some(operation);
            None
        }
    };
    if let Some(reason) = rejected {
        return Ok((atoms::error(), reason).encode(env));
    }
    let control = running.operation.clone();
    let pid = env.pid();
    let output = OwnedEnv::new();
    let token = output.save(token);
    let runtime = client.runtime.clone();
    let work = Submitted {
        running,
        output,
        token,
        pid,
        deadline,
        cancelled,
        stopping: runtime.stopping(),
    };
    let started = match request {
        request @ (Request::Execute(_) | Request::Insert(_, _, _)) => {
            let future = perform_statement(client, request, deadline, control);
            runtime.spawn(work.run(future, false, |result, _, late| {
                if let Some(error) = late {
                    *result = Err(error);
                }
            }))
        }
        request => {
            let context = runtime.context.clone();
            let future = perform_stream(client, request, context.clone(), deadline, control);
            runtime.spawn(
                work.run(future, finish_stream, move |result, client, late| {
                    if let Some(error) = late {
                        if let Ok(Value::Writer(writer)) = result {
                            writer.closed.store(true, Ordering::Release);
                            context.writers().remove(&writer.id);
                        }
                        *result = Err(error);
                    }
                    if result.is_err() {
                        if let Some(writer) = writer {
                            writer.closed.store(true, Ordering::Release);
                            if !client.runtime.is_draining() {
                                context.writers().remove(&writer.id);
                            }
                        }
                    }
                }),
            )
        }
    };
    started.map_err(|_| rustler::Error::Term(Box::new("runtime stopped".to_string())))?;
    Ok((atoms::ok(), operation_resource).encode(env))
}

struct Submitted {
    running: Running,
    output: OwnedEnv,
    token: SavedTerm,
    pid: LocalPid,
    deadline: tokio::time::Instant,
    cancelled: tokio::sync::watch::Receiver<bool>,
    stopping: tokio::sync::watch::Receiver<runtime::Shutdown>,
}

impl Submitted {
    async fn run<F, C>(self, future: F, finish_stream: bool, cleanup: C)
    where
        F: std::future::Future<Output = Result<Value, Failure>>,
        C: FnOnce(&mut Result<Value, Failure>, &GreptimeResource, Option<Failure>),
    {
        let Self {
            running,
            output,
            token,
            pid,
            deadline,
            mut cancelled,
            mut stopping,
        } = self;
        let control = running.operation.clone();
        let mut result = {
            let future = AssertUnwindSafe(future).catch_unwind();
            tokio::pin!(future);
            tokio::select! {
                biased;
                _ = runtime::wait_for_shutdown(&mut stopping, finish_stream) => Err(Failure::Closed),
                _ = cancelled.changed() => Err(Failure::Cancelled),
                _ = tokio::time::sleep_until(deadline) => Err(Failure::Timeout),
                value = &mut future => value.unwrap_or(Err(Failure::Panic)),
            }
        };
        control.wait_codec_jobs().await;
        cleanup(
            &mut result,
            &running.client,
            check_operation(deadline, &control).err(),
        );
        let large_reply = matches!(&result, Ok(Value::Batches(batches))
                if !small_query_reply(batches));
        let (mut output, token, encoded) = if large_reply {
            let fallback = OwnedEnv::new();
            let fallback_token = output.run(|env| fallback.save(token.load(env)));
            let encoded = running
                .client
                .runtime
                .codec_pool
                .run(control.clone(), deadline, move || {
                    Ok(encode_reply(output, token, result))
                })
                .await;
            control.wait_codec_jobs().await;
            match encoded {
                Ok(reply) => reply,
                Err(error) => encode_reply(fallback, fallback_token, Err(error)),
            }
        } else {
            encode_reply(output, token, result)
        };
        drop(running);
        let _ = output.send_and_clear(&pid, |env| {
            (atoms::greptimedb_nif(), token.load(env), encoded.load(env)).encode(env)
        });
    }
}

fn small_query_reply(batches: &[arrow::record_batch::RecordBatch]) -> bool {
    use arrow::datatypes::DataType;

    if batches.len() > 256 {
        return false;
    }
    let mut remaining_rows = 256;
    let mut remaining_bytes = 16 * 1024;
    for batch in batches {
        if batch.num_columns() > 8 || batch.num_rows() > remaining_rows {
            return false;
        }
        remaining_rows -= batch.num_rows();
        for column in batch.columns() {
            // Flat arrays have a bounded number of buffers to inspect. Shared
            // buffer capacity may overestimate bytes and safely select codec work.
            let dtype = column.data_type();
            if !(dtype.is_primitive()
                || matches!(
                    dtype,
                    DataType::Null
                        | DataType::Boolean
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Binary
                        | DataType::LargeBinary
                ))
            {
                return false;
            }
            let bytes = column.get_array_memory_size();
            if bytes > remaining_bytes {
                return false;
            }
            remaining_bytes -= bytes;
        }
    }
    true
}

fn encode_reply(
    output: OwnedEnv,
    token: SavedTerm,
    result: Result<Value, Failure>,
) -> (OwnedEnv, SavedTerm, SavedTerm) {
    let encoded = std::panic::catch_unwind(AssertUnwindSafe(|| {
        output.run(|env| {
            output.save(match &result {
                Ok(value) => value.encode(env),
                Err(error) => error.encode(env),
            })
        })
    }))
    .unwrap_or_else(|_| output.run(|env| output.save(Failure::Panic.encode(env))));
    (output, token, encoded)
}

#[rustler::nif]
fn codec_counts() -> (usize, usize, usize, usize, usize) {
    codec::counts()
}

#[rustler::nif]
fn cancel(operation: ResourceArc<OperationResource>) -> Atom {
    operation.0.cancel();
    atoms::ok()
}

#[rustler::nif]
fn operation_active(operation: ResourceArc<OperationResource>) -> bool {
    operation.0.active.load(Ordering::Acquire)
}

#[rustler::nif]
fn discard_writer(writer: ResourceArc<StreamWriterResource>) -> Atom {
    writer.closed.store(true, Ordering::Release);
    writer.runtime.discard(writer.id);
    atoms::ok()
}

#[rustler::nif]
fn resource_counts() -> (usize, usize) {
    (runtime::count(), operation::count())
}

fn check_operation(deadline: tokio::time::Instant, control: &Operation) -> Result<(), Failure> {
    if *control.cancel.borrow() {
        Err(Failure::Cancelled)
    } else if tokio::time::Instant::now() >= deadline {
        Err(Failure::Timeout)
    } else {
        Ok(())
    }
}

async fn perform_statement(
    client: ResourceArc<GreptimeResource>,
    request: Request,
    deadline: tokio::time::Instant,
    control: Arc<Operation>,
) -> Result<Value, Failure> {
    use futures::StreamExt;
    use greptimedb_ingester::api::v1::{ColumnSchema, RowInsertRequest, RowInsertRequests, Rows};
    let check = || check_operation(deadline, &control);
    check()?;
    match request {
        Request::Execute(sql) => {
            let mut stream = client.db.query(&sql).await.map_err(|error| {
                client.invalidate_channels_on(&error);
                Failure::native(error, ErrorStage::Query, WriteOutcome::Unknown)
            })?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                check()?;
                batches.push(batch.map_err(|error| {
                    client.invalidate_channels_on(&error);
                    Failure::native(error, ErrorStage::Query, WriteOutcome::Unknown)
                })?);
            }
            Ok(Value::Batches(batches))
        }
        Request::Insert(table, input, timestamp_type) => {
            if input.is_empty() {
                return Ok(Value::Count(0));
            }
            let server = match describe_table(&client, &table).await? {
                TableSchemaLookup::Found(schema) => Some(schema),
                TableSchemaLookup::NotFound | TableSchemaLookup::UnsupportedColumnType(_) => None,
            };
            check()?;
            let ts_column = client.ts_column.clone();
            let inline = match &server {
                Some(schema)
                    if input.small
                        && schema.columns().len() <= 8
                        && schema
                            .columns()
                            .iter()
                            .all(|column| column.name.len() <= 128) =>
                {
                    input.convert(|rows| util::try_encode_known_rows(schema, rows))?
                }
                _ => None,
            };
            let (schema, rows) = if let Some(value) = inline {
                value
            } else {
                client
                    .runtime
                    .codec_pool
                    .run(control.clone(), deadline, move || {
                        input.convert(|rows| match server {
                            Some(schema) => {
                                let columns: Vec<ColumnSchema> = schema
                                    .columns()
                                    .iter()
                                    .map(|c| ColumnSchema {
                                        column_name: c.name.clone(),
                                        datatype: c.data_type as i32,
                                        semantic_type: c.semantic_type as i32,
                                        ..Default::default()
                                    })
                                    .collect();
                                let columns = util::merge_inferred_columns(columns, &rows)?;
                                let rows = util::terms_to_proto_rows_with_columns(&columns, rows)?;
                                Ok((columns, rows))
                            }
                            None => util::terms_to_schema_and_rows(
                                rows,
                                ts_column.as_deref().unwrap_or("ts"),
                                timestamp_type,
                            ),
                        })
                    })
                    .await?
            };
            let request = RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: table,
                    rows: Some(Rows { schema, rows }),
                }],
            };
            check()?;
            let count = if let Some(ttl) = &client.ttl {
                client
                    .db
                    .insert_with_hints(request, &[("ttl", ttl.as_str())])
                    .await
                    .map_err(|error| {
                        client.invalidate_channels_on(&error);
                        let write_outcome = write_outcome_for_sdk_error(&error);
                        Failure::native(error, ErrorStage::Insert, write_outcome)
                    })?
            } else {
                client.db.insert(request).await.map_err(|error| {
                    client.invalidate_channels_on(&error);
                    let write_outcome = write_outcome_for_sdk_error(&error);
                    Failure::native(error, ErrorStage::Insert, write_outcome)
                })?
            };
            Ok(Value::Count(count))
        }
        _ => Err(Failure::BadArg),
    }
}

async fn perform_stream(
    client: ResourceArc<GreptimeResource>,
    request: Request,
    context: Arc<Context>,
    deadline: tokio::time::Instant,
    control: Arc<Operation>,
) -> Result<Value, Failure> {
    let check = || check_operation(deadline, &control);
    check()?;
    match request {
        Request::StreamStart(table) => {
            let schema = match describe_table(&client, &table).await? {
                TableSchemaLookup::Found(schema) => schema,
                TableSchemaLookup::NotFound => {
                    return Err(Failure::Native(NativeFailure::table_not_found(
                        ErrorStage::StreamStart,
                    )))
                }
                TableSchemaLookup::UnsupportedColumnType(column_type) => {
                    return Err(Failure::message(format!(
                        "Unknown column type: {column_type}"
                    )))
                }
            };
            check()?;
            let mut inserter = BulkInserter::new(client.client.clone(), client.db.dbname());
            if let Some(auth) = &client.auth {
                inserter.set_auth(auth.clone());
            }
            let writer = inserter
                .create_bulk_stream_writer(
                    &schema,
                    Some(BulkWriteOptions::default().with_timeout(client.stream_timeout)),
                )
                .await
                .map_err(|error| {
                    client.invalidate_channels_on(&error);
                    Failure::native(error, ErrorStage::StreamStart, WriteOutcome::NotSent)
                })?;
            writer.alloc_rows_buffer(5000, 1024).map_err(|error| {
                client.invalidate_channels_on(&error);
                Failure::native(error, ErrorStage::StreamStart, WriteOutcome::NotSent)
            })?;
            let id = runtime::writer_id();
            context.insert_writer(id, writer);
            Ok(Value::Writer(ResourceArc::new(StreamWriterResource {
                id,
                schema,
                submitted_batches: AtomicUsize::new(0),
                runtime: client.runtime.clone(),
                closed: AtomicBool::new(false),
            })))
        }
        Request::StreamWrite(resource, input) => {
            // Reserve before conversion so shutdown only takes idle writers. If conversion
            // is interrupted, return the untouched writer for draining older batches.
            let reserved = context.reserve_writer(resource.id).ok_or_else(|| {
                if client.runtime.is_stopping() {
                    Failure::Closed
                } else {
                    Failure::Native(NativeFailure::local(
                        ErrorKind::Closed,
                        ErrorStage::StreamWrite,
                        WriteOutcome::NotSent,
                        "Writer is closed",
                    ))
                }
            })?;
            let schema = resource.schema.clone();
            let rows = if input.small
                && schema.columns().len() <= 8
                && schema
                    .columns()
                    .iter()
                    .all(|column| column.name.len() <= 128)
            {
                input.convert(|rows| util::terms_to_rows(&schema, rows))?
            } else {
                client
                    .runtime
                    .codec_pool
                    .run(control.clone(), deadline, move || {
                        input.convert(|rows| util::terms_to_rows(&schema, rows))
                    })
                    .await?
            };
            check()?;
            if resource.closed.load(Ordering::Acquire) {
                return Err(Failure::Native(NativeFailure::local(
                    ErrorKind::Closed,
                    ErrorStage::StreamWrite,
                    WriteOutcome::NotSent,
                    "Writer is closed",
                )));
            }
            let mut writer = reserved.into_writer().ok_or_else(|| {
                Failure::Native(NativeFailure::local(
                    ErrorKind::Closed,
                    ErrorStage::StreamWrite,
                    WriteOutcome::NotSent,
                    "Writer is closed",
                ))
            })?;
            let result = writer.write_rows_async(rows).await.map_err(|error| {
                client.invalidate_channels_on(&error);
                let write_outcome = write_outcome_for_sdk_error(&error);
                Failure::native(error, ErrorStage::StreamWrite, write_outcome)
            });
            if result.is_ok()
                && resource.submitted_batches.fetch_add(1, Ordering::Relaxed)
                    % STREAM_RESPONSE_DRAIN_INTERVAL
                    == STREAM_RESPONSE_DRAIN_INTERVAL - 1
            {
                // Response IDs are not exposed by this driver. Consume completed successes
                // below the SDK's 1024-entry cleanup threshold; its four in-flight requests
                // remain tracked for backpressure, errors and finish().
                let _ = writer.flush_completed_responses();
            }
            if !resource.closed.load(Ordering::Acquire) {
                context.insert_writer(resource.id, writer);
            }
            result?;
            Ok(Value::Ok)
        }
        Request::StreamCloseAll(resources) => {
            let writers: Vec<_> = resources
                .iter()
                .filter_map(|resource| {
                    resource.closed.store(true, Ordering::Release);
                    context.writers().remove(&resource.id)
                })
                .collect();
            let results =
                futures::future::join_all(writers.into_iter().map(|writer| writer.finish())).await;
            for result in results {
                result.map_err(|error| {
                    client.invalidate_channels_on(&error);
                    Failure::native(error, ErrorStage::StreamClose, WriteOutcome::Unknown)
                })?;
            }
            Ok(Value::Ok)
        }
        Request::StreamClose(resource) => {
            resource.closed.store(true, Ordering::Release);
            let writer = context.writers().remove(&resource.id);
            if let Some(writer) = writer {
                writer.finish().await.map_err(|error| {
                    client.invalidate_channels_on(&error);
                    Failure::native(error, ErrorStage::StreamClose, WriteOutcome::Unknown)
                })?;
            }
            Ok(Value::Ok)
        }
        _ => Err(Failure::BadArg),
    }
}

rustler::init!("greptimedb_rs_nif", load = load);

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn transport_errors_preserve_retry_facts() {
        let error = greptimedb_ingester::Error::from(tonic::Status::unknown("transport error"));
        let failure =
            NativeFailure::from_greptimedb(error, ErrorStage::Insert, WriteOutcome::Unknown);

        assert_eq!(failure.kind, ErrorKind::Transport);
        assert_eq!(failure.source, ErrorSource::Server);
        assert_eq!(failure.code, ErrorCode::Unknown);
        assert_eq!(failure.stage, ErrorStage::Insert);
        assert_eq!(failure.write_outcome, WriteOutcome::Unknown);
        assert!(failure.retry_hint);
        assert!(failure.message.contains("transport error"));
    }

    #[test]
    fn h2_response_stream_disconnect_is_transport() {
        let error = greptimedb_ingester::Error::from(tonic::Status::unknown(
            "h2 protocol error: error reading a body from connection",
        ));
        let failure =
            NativeFailure::from_greptimedb(error, ErrorStage::Query, WriteOutcome::Unknown);

        assert_eq!(failure.kind, ErrorKind::Transport);
        assert_eq!(failure.code, ErrorCode::Unknown);
        assert_eq!(failure.stage, ErrorStage::Query);
        assert_eq!(failure.write_outcome, WriteOutcome::Unknown);
    }

    #[test]
    fn h2_and_connection_messages_are_transport_for_any_status_code() {
        for (code, message) in [
            (
                tonic::Code::Internal,
                "h2 protocol error: connection error received",
            ),
            (tonic::Code::Cancelled, "h2 protocol error: stream reset"),
            (tonic::Code::Unknown, "connection error detected"),
        ] {
            let error = greptimedb_ingester::Error::from(tonic::Status::new(code, message));
            let failure =
                NativeFailure::from_greptimedb(error, ErrorStage::Query, WriteOutcome::Unknown);

            assert_eq!(failure.kind, ErrorKind::Transport, "{code:?}: {message}");
        }
    }

    #[test]
    fn malformed_put_result_metadata_is_protocol_after_write() {
        let error = greptimedb_ingester::Error::SerdeJson {
            error: serde_json::from_str::<serde_json::Value>("{").unwrap_err(),
            location: snafu::Location::default(),
        };

        assert_eq!(error_kind(&error), ErrorKind::Protocol);
        assert_eq!(write_outcome_for_sdk_error(&error), WriteOutcome::Unknown);
    }

    #[test]
    fn describe_table_quotes_and_escapes_identifier() {
        assert_eq!(
            describe_table_sql("sensor-data"),
            "DESCRIBE \"sensor-data\""
        );
        assert_eq!(
            describe_table_sql("sensor\"data"),
            "DESCRIBE \"sensor\"\"data\""
        );
    }

    #[test]
    fn cancelled_server_error_is_not_reported_as_internal() {
        let error =
            greptimedb_ingester::Error::from(tonic::Status::cancelled("operation was canceled"));
        let failure =
            NativeFailure::from_greptimedb(error, ErrorStage::Metadata, WriteOutcome::NotSent);

        assert_eq!(failure.kind, ErrorKind::Cancelled);
        assert_eq!(failure.source, ErrorSource::Server);
        assert_eq!(failure.code, ErrorCode::Cancelled);
        assert_eq!(failure.stage, ErrorStage::Metadata);
        assert_eq!(failure.write_outcome, WriteOutcome::NotSent);
    }

    #[test]
    fn not_found_errors_remain_distinct_from_transport_errors() {
        let error = greptimedb_ingester::Error::from(tonic::Status::not_found("table not found"));
        let failure =
            NativeFailure::from_greptimedb(error, ErrorStage::Metadata, WriteOutcome::NotSent);

        assert_eq!(failure.kind, ErrorKind::Server);
        assert_eq!(failure.code, ErrorCode::NotFound);
        assert_eq!(failure.stage, ErrorStage::Metadata);
        assert_eq!(failure.write_outcome, WriteOutcome::NotSent);
    }

    #[test]
    fn only_table_not_found_server_code_triggers_missing_table_fallback() {
        let table_not_found = greptimedb_ingester::Error::from(status_with_greptime_code(
            tonic::Code::NotFound,
            "4001",
            "table not found",
        ));
        assert!(is_table_not_found_error(&table_not_found));

        for server_code in ["4002", "4004", "4005"] {
            let error = greptimedb_ingester::Error::from(status_with_greptime_code(
                tonic::Code::NotFound,
                server_code,
                "resource not found",
            ));
            assert!(
                !is_table_not_found_error(&error),
                "unexpected missing-table fallback for {server_code}"
            );
        }

        let missing_code =
            greptimedb_ingester::Error::from(tonic::Status::not_found("table not found"));
        assert!(!is_table_not_found_error(&missing_code));
    }

    #[test]
    fn small_query_reply_bounds_rows_columns_bytes_and_batches() {
        use arrow::array::{ArrayRef, BinaryArray, Int64Array, ListArray};
        use arrow::buffer::{Buffer, OffsetBuffer};
        use arrow::datatypes::Int64Type;
        use arrow::record_batch::RecordBatch;

        let numeric = |rows, columns| {
            RecordBatch::try_from_iter((0..columns).map(|i| {
                (
                    format!("c{i}"),
                    Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef,
                )
            }))
            .unwrap()
        };
        assert!(small_query_reply(&[numeric(256, 1)]));
        assert!(!small_query_reply(&[numeric(257, 1)]));
        assert!(!small_query_reply(&[numeric(128, 1), numeric(129, 1)]));
        assert!(small_query_reply(&[numeric(1, 8)]));
        assert!(!small_query_reply(&[numeric(1, 9)]));

        let binary = |bytes| {
            let array = BinaryArray::new(
                OffsetBuffer::new(vec![0, bytes as i32].into()),
                Buffer::from(vec![0u8; bytes]),
                None,
            );
            RecordBatch::try_from_iter([("payload", Arc::new(array) as ArrayRef)]).unwrap()
        };
        let overhead = binary(0).column(0).get_array_memory_size();
        assert!(small_query_reply(&[binary(16 * 1024 - overhead)]));
        assert!(!small_query_reply(&[binary(16 * 1024 - overhead + 1)]));
        assert!(!small_query_reply(&[binary(8 * 1024), binary(8 * 1024)]));

        let empty = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
        assert!(small_query_reply(&vec![empty.clone(); 256]));
        assert!(!small_query_reply(&vec![empty; 257]));
        let nested = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1)])]);
        let nested =
            RecordBatch::try_from_iter([("nested", Arc::new(nested) as ArrayRef)]).unwrap();
        assert!(!small_query_reply(&[nested]));
    }

    #[test]
    fn poisoned_client_slot_closes_and_cancels_without_finishing_early() {
        let owner = RuntimeResource(runtime::start(1, 1).expect("start test runtime"));
        for (occupied, finish_stream) in [(false, false), (true, false), (true, true)] {
            let sdk = Client::with_urls(["127.0.0.1:4001"]);
            let (operation, _cancelled) = Operation::new(finish_stream);
            let client = GreptimeResource {
                db: Database::new_with_dbname("public", sdk.clone()),
                client: sdk,
                channel_manager: ChannelManager::new(),
                auth: None,
                ts_column: None,
                ttl: None,
                stream_timeout: Duration::from_secs(30),
                runtime: owner.0.clone(),
                operation: Mutex::new(occupied.then(|| operation.clone())),
                closed: AtomicBool::new(false),
            };
            client.drain();
            assert!(client.closed.load(Ordering::Acquire));
            assert_eq!(*operation.cancel.borrow(), occupied && !finish_stream);
            assert!(operation.active.load(Ordering::Acquire));
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let _slot = client.operation.lock().expect("unpoisoned test slot");
                panic!("poison client slot");
            }));
            let result = catch_unwind(AssertUnwindSafe(|| client.close()));
            let cancelled = *operation.cancel.borrow();
            let still_active = operation.active.load(Ordering::Acquire);
            operation.finish();
            assert!(result.is_ok());
            assert!(client.closed.load(Ordering::Acquire));
            assert!(!client.operation.is_poisoned());
            assert_eq!(cancelled, occupied);
            assert!(still_active);
            assert!(catch_unwind(AssertUnwindSafe(|| client.close())).is_ok());
        }
        runtime::stop(&owner);
    }

    fn status_with_greptime_code(
        code: tonic::Code,
        server_code: &str,
        message: &str,
    ) -> tonic::Status {
        let mut status = tonic::Status::new(code, message);
        status.metadata_mut().insert(
            "x-greptime-err-code",
            tonic::metadata::MetadataValue::try_from(server_code).unwrap(),
        );
        status
    }
}
