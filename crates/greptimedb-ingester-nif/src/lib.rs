use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use greptime_proto::v1::auth_header::AuthScheme;
use greptime_proto::v1::Basic;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::{
    BulkInserter, BulkStreamWriter, BulkWriteOptions, ColumnDataType, TableSchema,
};
use rustler::{Atom, Encoder, Env, NifResult, ResourceArc, Term};
use tokio::runtime::Runtime;

static FIPS_ENABLED: AtomicBool = AtomicBool::new(false);

pub mod atoms;
mod types;
mod util;

pub struct GreptimeResource {
    pub db: Database,
    pub client: Client,
    pub auth: Option<AuthScheme>,
    pub ts_column: Option<String>,
    pub ttl: Option<String>,
    pub runtime: Arc<Runtime>, // Per-connection runtime
    defaults_cache: Mutex<DefaultsCache>,
}

struct CachedColumnDefaults {
    fetched_at: Instant,
    fingerprint: String,
    values: std::collections::HashMap<String, util::ColumnDefault>,
}

struct DefaultsCache {
    entries: std::collections::HashMap<String, CachedColumnDefaults>,
    max_entries: usize,
}

impl DefaultsCache {
    const MAX_ENTRIES: usize = 64;
    const TTL: Duration = Duration::from_secs(5);

    fn new() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            max_entries: Self::MAX_ENTRIES,
        }
    }

    fn evict_expired(&mut self) {
        self.entries
            .retain(|_, entry| entry.fetched_at.elapsed() < Self::TTL);
    }

    fn insert(&mut self, key: String, entry: CachedColumnDefaults) {
        self.evict_expired();
        if !self.entries.contains_key(&key) && self.entries.len() >= self.max_entries {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.fetched_at)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(key, entry);
    }
}

// Wrapper to force Send/Sync on BulkStreamWriter
pub struct SendableBulkStreamWriter(pub BulkStreamWriter);
unsafe impl Send for SendableBulkStreamWriter {}
unsafe impl Sync for SendableBulkStreamWriter {}

pub struct StreamWriterResource {
    pub writer: tokio::sync::Mutex<Option<SendableBulkStreamWriter>>,
    pub schema: TableSchema,
    pub runtime: Arc<Runtime>, // Need runtime for async operations
}

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    FIPS_ENABLED.store(check_fips_enabled(), Ordering::Relaxed);
    _ = rustler::resource!(GreptimeResource, env);
    _ = rustler::resource!(StreamWriterResource, env);
    true
}

fn check_fips_enabled() -> bool {
    std::fs::read_to_string("/proc/sys/crypto/fips_enabled")
        .map(|s| s.trim() == "1")
        .unwrap_or(false)
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

    // Create a new Tokio runtime for this connection
    let runtime = Runtime::new().map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
    let runtime = Arc::new(runtime);

    let endpoints_term: Term = opts.map_get(atoms::endpoints().to_term(env))?;
    let dbname_term: Term = opts.map_get(atoms::dbname().to_term(env))?;

    let endpoints: Vec<String> = endpoints_term.decode()?;
    let dbname: String = dbname_term.decode()?;

    let client = if let Ok(tls_term) = opts.map_get(atoms::tls().to_term(env)) {
        if tls_term.decode::<bool>()? {
            let tls_verify = match opts.map_get(atoms::verify().to_term(env)) {
                Ok(verify_term) => match decode_tls_verify(verify_term) {
                    Ok(verify) => verify,
                    Err(err) => return Ok((atoms::error(), err).encode(env)),
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

            match Client::with_tls_and_urls_with_verify(endpoints, tls_option, tls_verify) {
                Ok(c) => c,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            }
        } else {
            Client::with_urls(endpoints)
        }
    } else {
        Client::with_urls(endpoints)
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
        auth,
        ts_column,
        ttl,
        runtime,
        defaults_cache: Mutex::new(DefaultsCache::new()),
    });
    Ok((atoms::ok(), resource).encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn disconnect(env: Env, _resource: ResourceArc<GreptimeResource>) -> NifResult<Term> {
    // The resource will be automatically dropped when this function returns
    // and all Arc references are released. The Tokio runtime will be shut down
    // gracefully when the last Arc<Runtime> is dropped.
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn execute(env: Env, resource: ResourceArc<GreptimeResource>, sql: String) -> NifResult<Term> {
    let db = &resource.db;
    let runtime = &resource.runtime;

    // Collect RecordBatches first (Env is not Send)
    let result = runtime.block_on(async {
        use futures::StreamExt;
        match db.query(&sql).await {
            Ok(mut stream) => {
                let mut batches = Vec::new();
                while let Some(batch_res) = stream.next().await {
                    match batch_res {
                        Ok(batch) => {
                            batches.push(batch);
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(batches)
            }
            Err(e) => Err(e),
        }
    });

    match result {
        Ok(batches) => {
            let mut all_rows = Vec::new();
            for batch in batches {
                let rows = types::record_batch_to_terms(env, &batch);
                all_rows.extend(rows);
            }
            Ok((atoms::ok(), all_rows).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

async fn fetch_table_schema(
    db: &Database,
    table_name: &str,
) -> Result<(TableSchema, std::collections::HashSet<String>, String), String> {
    let sql = format!("DESCRIBE {table_name}");
    let mut stream = db.query(&sql).await.map_err(|e| e.to_string())?;

    let mut table_schema = TableSchema::builder()
        .name(table_name)
        .build()
        .map_err(|e| e.to_string())?;
    let mut json2_columns = std::collections::HashSet::new();
    let mut fingerprint = String::new();
    let mut seen_columns = 0;

    while let Some(batch_res) = futures::StreamExt::next(&mut stream).await {
        let batch = batch_res.map_err(|e| e.to_string())?;

        // 0: Field, 1: Type, 2: Null, 3: Key, 4: Default, 5: Semantic Type
        if batch.num_columns() < 6 {
            return Err("DESCRIBE result has unexpected number of columns".to_string());
        }
        seen_columns += batch.num_rows();

        for i in 0..batch.num_rows() {
            let name = string_value(batch.column(0).as_ref(), i)
                .ok_or("DESCRIBE returned a NULL column name")?;
            let type_str = string_value(batch.column(1).as_ref(), i)
                .ok_or("DESCRIBE returned a NULL column type")?;
            let semantic_str = string_value(batch.column(5).as_ref(), i)
                .ok_or("DESCRIBE returned a NULL semantic type")?;
            let default = string_value(batch.column(4).as_ref(), i).unwrap_or_default();
            fingerprint.push_str(name);
            fingerprint.push('\u{1f}');
            fingerprint.push_str(type_str);
            fingerprint.push('\u{1f}');
            fingerprint.push_str(semantic_str);
            fingerprint.push('\u{1f}');
            fingerprint.push_str(default);
            fingerprint.push('\u{1e}');

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
                // Legacy JSON (jsonb) column.
                "Json" => ColumnDataType::Json,
                // JSON2 column.
                _ if type_str.starts_with("Json2") => {
                    json2_columns.insert(name.to_string());
                    ColumnDataType::Json
                }
                _ => return Err(format!("Unknown column type: {type_str}")),
            };

            match semantic_str {
                "TAG" => table_schema = table_schema.add_tag(name, dtype),
                "FIELD" => table_schema = table_schema.add_field(name, dtype),
                "TIMESTAMP" => table_schema = table_schema.add_timestamp(name, dtype),
                _ => {}
            }
        }
    }

    if seen_columns == 0 {
        return Err(format!("no columns found for table `{table_name}`"));
    }
    Ok((table_schema, json2_columns, fingerprint))
}

async fn fetch_column_defaults(
    db: &Database,
    table_name: &str,
) -> Result<std::collections::HashMap<String, util::ColumnDefault>, String> {
    let (db_catalog, _) = split_dbname(db.dbname());
    let (catalog, schema_name, bare_table_name) = match table_name.rsplit_once('.') {
        Some((schema_part, table)) => {
            let (catalog, schema) = split_dbname(schema_part);
            (catalog.or(db_catalog), schema, table)
        }
        None => {
            let (catalog, schema) = split_dbname(db.dbname());
            (catalog, schema, table_name)
        }
    };
    let where_clause = match catalog {
        Some(catalog) => format!(
            "table_catalog = {} AND table_schema = {} AND table_name = {}",
            sql_string_literal(catalog),
            sql_string_literal(schema_name),
            sql_string_literal(bare_table_name)
        ),
        None => format!(
            "table_schema = {} AND table_name = {}",
            sql_string_literal(schema_name),
            sql_string_literal(bare_table_name)
        ),
    };
    let sql = format!(
        "SELECT column_name, column_default FROM information_schema.columns \
         WHERE {where_clause} ORDER BY ordinal_position"
    );
    let mut stream = db.query(&sql).await.map_err(|e| e.to_string())?;
    let mut default_values: std::collections::HashMap<String, util::ColumnDefault> =
        std::collections::HashMap::new();
    let mut seen_columns = 0;
    while let Some(batch_res) = futures::StreamExt::next(&mut stream).await {
        let batch = batch_res.map_err(|e| e.to_string())?;
        if batch.num_columns() < 2 {
            return Err(
                "information_schema.columns result has unexpected number of columns".to_string(),
            );
        }
        seen_columns += batch.num_rows();
        for i in 0..batch.num_rows() {
            let name = string_value(batch.column(0).as_ref(), i)
                .ok_or("information_schema returned a NULL column name")?;
            let Some(default) = string_value(batch.column(1).as_ref(), i) else {
                continue;
            };
            default_values.insert(
                name.to_string(),
                util::ColumnDefault::Value(default.to_string()),
            );
        }
    }
    if seen_columns == 0 {
        return Err(format!(
            "no column defaults found for table `{table_name}` in catalog/schema `{}`",
            db.dbname()
        ));
    }

    let null_columns: Vec<String> = default_values
        .iter()
        .filter(
            |(_, default)| matches!(default, util::ColumnDefault::Value(value) if value == "Null"),
        )
        .map(|(name, _)| name.clone())
        .collect();
    if !null_columns.is_empty() {
        let ddl = fetch_create_table_ddl(db, table_name).await?;
        let null_defaults = sql_null_defaults(&ddl, &null_columns)?;
        for name in null_columns {
            let is_sql_null = null_defaults
                .get(&name)
                .copied()
                .ok_or_else(|| format!("could not disambiguate the default for column `{name}`"))?;
            let default = if is_sql_null {
                util::ColumnDefault::Null
            } else {
                util::ColumnDefault::Value("Null".to_string())
            };
            default_values.insert(name, default);
        }
    }
    Ok(default_values)
}

fn cached_column_defaults(
    resource: &GreptimeResource,
    runtime: &Runtime,
    table: &str,
    fingerprint: &str,
) -> Result<std::collections::HashMap<String, util::ColumnDefault>, String> {
    let cache_key = format!("{}|{}", resource.db.dbname(), table);
    {
        let mut cache = resource
            .defaults_cache
            .lock()
            .map_err(|_| "column defaults cache is poisoned".to_string())?;
        cache.evict_expired();
        if let Some(entry) = cache.entries.get(&cache_key) {
            if entry.fingerprint == fingerprint {
                return Ok(entry.values.clone());
            }
        }
    }

    let values = runtime.block_on(fetch_column_defaults(&resource.db, table))?;
    // Do not cache ambiguous/empty defaults; they can change without changing
    // the DESCRIBE-rendered default hint.
    let ambiguous = values.values().any(|default| match default {
        util::ColumnDefault::Null => true,
        util::ColumnDefault::Value(value) => value.is_empty() || value == "Null",
    });
    if ambiguous {
        return Ok(values);
    }

    let mut cache = resource
        .defaults_cache
        .lock()
        .map_err(|_| "column defaults cache is poisoned".to_string())?;
    cache.insert(
        cache_key,
        CachedColumnDefaults {
            fetched_at: Instant::now(),
            fingerprint: fingerprint.to_string(),
            values: values.clone(),
        },
    );
    Ok(values)
}

async fn fetch_create_table_ddl(db: &Database, table_name: &str) -> Result<String, String> {
    let sql = format!("SHOW CREATE TABLE {table_name}");
    let mut stream = db.query(&sql).await.map_err(|e| e.to_string())?;
    while let Some(batch_res) = futures::StreamExt::next(&mut stream).await {
        let batch = batch_res.map_err(|e| e.to_string())?;
        if batch.num_columns() >= 2 && batch.num_rows() > 0 {
            if let Some(ddl) = string_value(batch.column(1).as_ref(), 0) {
                return Ok(ddl.to_string());
            }
        }
    }
    Err(format!(
        "SHOW CREATE TABLE returned no DDL for table `{table_name}`"
    ))
}

fn sql_null_defaults(
    ddl: &str,
    columns: &[String],
) -> Result<std::collections::HashMap<String, bool>, String> {
    let mut result = std::collections::HashMap::new();
    for line in ddl.lines() {
        let trimmed = line.trim_start();
        for name in columns {
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            if !trimmed.starts_with(&quoted) {
                continue;
            }
            let rest = &trimmed[quoted.len()..];
            if !rest.starts_with(char::is_whitespace) {
                continue;
            }
            let Some(position) = rest.find(" DEFAULT ") else {
                continue;
            };
            let default = rest[position + " DEFAULT ".len()..].trim_start();
            let is_sql_null = if default
                .get(..4)
                .is_some_and(|token| token.eq_ignore_ascii_case("NULL"))
                && !default[4..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                true
            } else if default.starts_with('\'') {
                false
            } else {
                return Err(format!(
                    "unsupported default rendering for column `{name}`: {default}"
                ));
            };
            result.insert(name.clone(), is_sql_null);
        }
    }
    for name in columns {
        if !result.contains_key(name) {
            return Err(format!(
                "could not find a DEFAULT clause for column `{name}` in SHOW CREATE TABLE"
            ));
        }
    }
    Ok(result)
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn split_dbname(dbname: &str) -> (Option<&str>, &str) {
    match dbname.split_once('-') {
        Some((catalog, schema)) => (Some(catalog), schema),
        None => (None, dbname),
    }
}

fn string_value(array: &dyn arrow::array::Array, index: usize) -> Option<&str> {
    use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        (!array.is_null(index)).then(|| array.value(index))
    } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        (!array.is_null(index)).then(|| array.value(index))
    } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        (!array.is_null(index)).then(|| array.value(index))
    } else {
        None
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn insert<'a>(
    env: Env<'a>,
    resource: ResourceArc<GreptimeResource>,
    table: String,
    rows_term: Vec<Term<'a>>,
) -> NifResult<Term<'a>> {
    if rows_term.is_empty() {
        return Ok((atoms::ok(), 0).encode(env));
    }

    let runtime = &resource.runtime;

    use greptimedb_ingester::api::v1::{RowInsertRequest, RowInsertRequests, Rows};

    // 1. Try Fetch Schema from Server
    let table_schema_res = runtime.block_on(fetch_table_schema(&resource.db, &table));

    let (inserts, auto_created_with_json2) = match table_schema_res {
        Ok((s, json2_columns, fingerprint)) => {
            // Table exists: keep the server's columns and their types, then merge in
            // the columns inferred from the input rows. GreptimeDB adds unknown
            // columns automatically, so new input fields are no longer dropped.
            use greptimedb_ingester::api::v1::ColumnSchema;
            let server_columns: Vec<ColumnSchema> = s
                .columns()
                .iter()
                .map(|c| {
                    if json2_columns.contains(&c.name) {
                        // JSON2 column: native JSON type extension and the
                        // `greptime.json2` column options.
                        greptimedb_ingester::helpers::schema::json2_field(&c.name)
                    } else {
                        ColumnSchema {
                            column_name: c.name.clone(),
                            datatype: c.data_type as i32,
                            semantic_type: c.semantic_type as i32,
                            ..Default::default()
                        }
                    }
                })
                .collect();

            let inserts =
                if let Some(plan) = util::analyze_known_columns(&server_columns, &rows_term)? {
                    let schema: Vec<ColumnSchema> = plan
                        .column_indices
                        .iter()
                        .map(|&index| server_columns[index].clone())
                        .collect();
                    let defaults = if plan.all_same {
                        std::collections::HashMap::new()
                    } else {
                        cached_column_defaults(&resource, runtime, &table, &fingerprint)
                            .map_err(|e| rustler::Error::RaiseTerm(Box::new(e)))?
                    };
                    let rows = util::terms_to_proto_rows_from_plan(
                        env,
                        &server_columns,
                        &plan.column_indices,
                        &plan.row_terms,
                        &plan.row_masks,
                        plan.words,
                        &defaults,
                    )?;
                    vec![RowInsertRequest {
                        table_name: table.clone(),
                        rows: Some(Rows { schema, rows }),
                    }]
                } else {
                    let merged = util::merge_inferred_columns(server_columns, &rows_term)?;
                    let present = util::collect_present_columns(&rows_term)?;
                    let schema = util::retain_present_columns(&merged, &present);
                    let defaults = if util::rows_have_same_presence(&rows_term)? {
                        std::collections::HashMap::new()
                    } else {
                        cached_column_defaults(&resource, runtime, &table, &fingerprint)
                            .map_err(|e| rustler::Error::RaiseTerm(Box::new(e)))?
                    };
                    let rows = util::terms_to_proto_rows_with_columns_and_defaults(
                        &schema, rows_term, &defaults,
                    )?;
                    vec![RowInsertRequest {
                        table_name: table.clone(),
                        rows: Some(Rows { schema, rows }),
                    }]
                };
            (inserts, false)
        }
        Err(_) => {
            // Table might not exist, infer schema locally
            let (schema, rows) = util::terms_to_schema_and_rows(
                rows_term,
                resource.ts_column.as_deref().unwrap_or("ts"),
            )?;
            let has_json2 = schema.iter().any(util::is_json2_column);
            (
                vec![RowInsertRequest {
                    table_name: table.clone(),
                    rows: Some(Rows { schema, rows }),
                }],
                has_json2,
            )
        }
    };

    // 2. Construct Request
    let insert_request = RowInsertRequests { inserts };

    // 3. Insert using Database
    let result: Result<u32, String> = runtime.block_on(async {
        let mut hints: Vec<(&str, &str)> = Vec::new();
        if let Some(ttl) = &resource.ttl {
            hints.push(("ttl", ttl.as_str()));
        }
        if auto_created_with_json2 {
            // JSON2 columns require append-only tables; without the hint the
            // auto-created table would default to `merge_mode = 'last_row'`
            // and the server would reject the JSON2 column.
            hints.push(("append_mode", "true"));
        }
        if hints.is_empty() {
            resource
                .db
                .insert(insert_request)
                .await
                .map_err(|e| e.to_string())
        } else {
            resource
                .db
                .insert_with_hints(insert_request, &hints)
                .await
                .map_err(|e| e.to_string())
        }
    });

    match result {
        Ok(affected) => Ok((atoms::ok(), affected).encode(env)),
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn stream_start<'a>(
    env: Env<'a>,
    resource: ResourceArc<GreptimeResource>,
    table: String,
    _first_row: Term<'a>,
) -> NifResult<Term<'a>> {
    let runtime = &resource.runtime;

    // 1. Fetch Schema from Server
    let table_template_res = runtime.block_on(fetch_table_schema(&resource.db, &table));
    let (table_template, json2_columns, _fingerprint) = match table_template_res {
        Ok(s) => s,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };
    if !json2_columns.is_empty() {
        return Ok((
            atoms::error(),
            "stream (bulk) write does not support JSON2 columns; use insert/3 instead",
        )
            .encode(env));
    }

    let schema_clone = table_template.clone();

    let result: Result<ResourceArc<StreamWriterResource>, String> = runtime.block_on(async {
        let mut bulk_inserter = BulkInserter::new(resource.client.clone(), resource.db.dbname());
        if let Some(auth) = &resource.auth {
            bulk_inserter.set_auth(auth.clone());
        }

        // Create writer without compression for better CPU efficiency
        let writer = bulk_inserter
            .create_bulk_stream_writer(
                &table_template,
                Some(BulkWriteOptions::default().with_timeout(Duration::from_secs(30))),
            )
            .await
            .map_err(|e| e.to_string())?;

        // Pre-allocate buffer for optimal performance
        // capacity: 5000 rows (good for 5K-10K batch sizes)
        writer.alloc_rows_buffer(5000).map_err(|e| e.to_string())?;

        Ok(ResourceArc::new(StreamWriterResource {
            writer: tokio::sync::Mutex::new(Some(SendableBulkStreamWriter(writer))),
            schema: schema_clone,
            runtime: resource.runtime.clone(),
        }))
    });

    match result {
        Ok(res) => Ok((atoms::ok(), res).encode(env)),
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn stream_write<'a>(
    env: Env<'a>,
    resource: ResourceArc<StreamWriterResource>,
    rows_term: Vec<Term<'a>>,
) -> NifResult<Term<'a>> {
    let runtime = &resource.runtime;
    let greptime_rows = util::terms_to_rows(&resource.schema, rows_term)?;

    let result: Result<(), String> = runtime.block_on(async {
        let mut writer_guard = resource.writer.lock().await;
        if let Some(writer_wrapper) = writer_guard.as_mut() {
            let writer = &mut writer_wrapper.0;
            let _request_id = writer
                .write_rows_async(greptime_rows)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        } else {
            Err("Writer is closed".to_string())
        }
    });

    match result {
        Ok(_) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn stream_close(env: Env, resource: ResourceArc<StreamWriterResource>) -> NifResult<Term> {
    let runtime = &resource.runtime;
    let result: Result<(), String> = runtime.block_on(async {
        let mut writer_guard = resource.writer.lock().await;
        if let Some(writer_wrapper) = writer_guard.take() {
            let writer = writer_wrapper.0;
            writer.finish().await.map_err(|e| e.to_string())?;
            Ok(())
        } else {
            Ok(())
        }
    });

    match result {
        Ok(_) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_cache_evicts_expired_and_oldest_entries() {
        let mut cache = DefaultsCache::new();
        cache.max_entries = 2;
        let entry = |fingerprint: &str, fetched_at: Instant| CachedColumnDefaults {
            fetched_at,
            fingerprint: fingerprint.to_string(),
            values: std::collections::HashMap::new(),
        };

        cache.insert(
            "expired".to_string(),
            entry("expired", Instant::now() - Duration::from_secs(10)),
        );
        cache.evict_expired();
        assert!(cache.entries.is_empty());

        cache.insert(
            "a".to_string(),
            entry("a", Instant::now() - Duration::from_secs(4)),
        );
        cache.insert(
            "b".to_string(),
            entry("b", Instant::now() - Duration::from_secs(3)),
        );
        cache.insert(
            "c".to_string(),
            entry("c", Instant::now() - Duration::from_secs(2)),
        );
        assert_eq!(cache.entries.len(), 2);
        assert!(!cache.entries.contains_key("a"));
        assert!(cache.entries.contains_key("b"));
        assert!(cache.entries.contains_key("c"));
    }

    #[test]
    fn distinguishes_sql_null_from_string_null_in_ddl() {
        let ddl = r#"CREATE TABLE IF NOT EXISTS "t" (
  "s_sql_null" STRING NULL DEFAULT NULL,
  "s_cap_null" STRING NULL DEFAULT 'Null',
  TIME INDEX ("ts")
)"#;
        let columns = vec!["s_sql_null".to_string(), "s_cap_null".to_string()];
        let defaults = sql_null_defaults(ddl, &columns).expect("parse defaults");
        assert_eq!(defaults.get("s_sql_null"), Some(&true));
        assert_eq!(defaults.get("s_cap_null"), Some(&false));
    }
}

rustler::init!("greptimedb_rs_nif", load = load);
