use greptime_proto::v1::auth_header::AuthScheme;
use greptime_proto::v1::Basic;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::{
    BulkInserter, BulkStreamWriter, BulkWriteOptions, ColumnDataType, CompressionType, TableSchema,
};
use lazy_static::lazy_static;
use rustler::{Encoder, Env, NifResult, ResourceArc, Term};
use std::time::Duration;
use tokio::runtime::Runtime;

pub mod atoms;
mod types;
mod util;

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

pub struct GreptimeResource {
    pub db: Database,
    pub client: Client,
    pub auth: Option<AuthScheme>,
}

// Wrapper to force Send/Sync on BulkStreamWriter
pub struct SendableBulkStreamWriter(pub BulkStreamWriter);
unsafe impl Send for SendableBulkStreamWriter {}
unsafe impl Sync for SendableBulkStreamWriter {}

pub struct StreamWriterResource {
    pub writer: tokio::sync::Mutex<Option<SendableBulkStreamWriter>>,
    pub schema: TableSchema,
}

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    _ = rustler::resource!(GreptimeResource, env);
    _ = rustler::resource!(StreamWriterResource, env);
    true
}

use greptimedb_ingester::channel_manager::ClientTlsOption;

#[rustler::nif(schedule = "DirtyIo")]
fn connect(opts: Term) -> NifResult<Term> {
    let env = opts.get_env();

    let endpoints_term: Term = opts.map_get(atoms::endpoints().to_term(env))?;
    let dbname_term: Term = opts.map_get(atoms::dbname().to_term(env))?;

    let endpoints: Vec<String> = endpoints_term.decode()?;
    let dbname: String = dbname_term.decode()?;

    let client = if let Ok(tls_term) = opts.map_get(atoms::tls().to_term(env)) {
        if tls_term.decode::<bool>()? {
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
            };

            match Client::with_tls_and_urls(endpoints, tls_option) {
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

    if let Ok(username_term) = opts.map_get(atoms::username().to_term(env)) {
        if let Ok(password_term) = opts.map_get(atoms::password().to_term(env)) {
            let username: String = username_term.decode()?;
            let password: String = password_term.decode()?;
            let auth_scheme = AuthScheme::Basic(Basic { username, password });
            db.set_auth(auth_scheme.clone());
            auth = Some(auth_scheme);
        }
    }

    let resource = ResourceArc::new(GreptimeResource { db, client, auth });
    Ok((atoms::ok(), resource).encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn execute(env: Env, resource: ResourceArc<GreptimeResource>, sql: String) -> NifResult<Term> {
    let db = &resource.db;

    // Collect RecordBatches first (Env is not Send)
    let result = RUNTIME.block_on(async {
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

async fn fetch_table_schema(db: &Database, table_name: &str) -> Result<TableSchema, String> {
    let sql = format!("DESCRIBE {table_name}");
    let mut stream = db.query(&sql).await.map_err(|e| e.to_string())?;

    let mut table_schema = TableSchema::builder()
        .name(table_name)
        .build()
        .map_err(|e| e.to_string())?;

    while let Some(batch_res) = futures::StreamExt::next(&mut stream).await {
        let batch = batch_res.map_err(|e| e.to_string())?;

        // Ensure columns exist
        // 0: Field, 1: Type, 2: Null, 3: Key, 4: Default, 5: Semantic Type
        if batch.num_columns() < 6 {
            return Err("DESCRIBE result has unexpected number of columns".to_string());
        }

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("Failed to cast column 0 to StringArray")?;
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("Failed to cast column 1 to StringArray")?;
        let semantic_types = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("Failed to cast column 5 to StringArray")?;

        for i in 0..batch.num_rows() {
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

    Ok(table_schema)
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

    use greptimedb_ingester::api::v1::{RowInsertRequest, RowInsertRequests, Rows};

    // 1. Try Fetch Schema from Server
    let table_schema_res: Result<TableSchema, String> =
        RUNTIME.block_on(fetch_table_schema(&resource.db, &table));

    let (schema, rows) = match table_schema_res {
        Ok(s) => {
            // Table exists, use server schema
            let proto_rows = util::terms_to_proto_rows_using_schema(&s, rows_term)?;

            use greptimedb_ingester::api::v1::ColumnSchema;
            let schema_cols: Vec<ColumnSchema> = s
                .columns()
                .iter()
                .map(|c| ColumnSchema {
                    column_name: c.name.clone(),
                    datatype: c.data_type as i32,
                    semantic_type: c.semantic_type as i32,
                    ..Default::default()
                })
                .collect();

            (schema_cols, proto_rows)
        }
        Err(_) => {
            // Table might not exist, infer schema locally
            util::terms_to_schema_and_rows(rows_term)?
        }
    };

    // 2. Construct Request
    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table,
            rows: Some(Rows { schema, rows }),
        }],
    };

    // 3. Insert using Database
    let result: Result<u32, String> = RUNTIME.block_on(async {
        resource
            .db
            .insert(insert_request)
            .await
            .map_err(|e| e.to_string())
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
    // 1. Fetch Schema from Server
    let table_template_res: Result<TableSchema, String> =
        RUNTIME.block_on(fetch_table_schema(&resource.db, &table));
    let table_template = match table_template_res {
        Ok(s) => s,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let schema_clone = table_template.clone();

    let result: Result<ResourceArc<StreamWriterResource>, String> = RUNTIME.block_on(async {
        let mut bulk_inserter = BulkInserter::new(resource.client.clone(), resource.db.dbname());
        if let Some(auth) = &resource.auth {
            bulk_inserter.set_auth(auth.clone());
        }

        let writer = bulk_inserter
            .create_bulk_stream_writer(
                &table_template,
                Some(
                    BulkWriteOptions::default()
                        .with_compression(CompressionType::Zstd)
                        .with_timeout(Duration::from_secs(30)),
                ),
            )
            .await
            .map_err(|e| e.to_string())?;

        Ok(ResourceArc::new(StreamWriterResource {
            writer: tokio::sync::Mutex::new(Some(SendableBulkStreamWriter(writer))),
            schema: schema_clone,
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
    let greptime_rows = util::terms_to_rows(&resource.schema, rows_term)?;

    let result: Result<(), String> = RUNTIME.block_on(async {
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
    let result: Result<(), String> = RUNTIME.block_on(async {
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

rustler::init!("greptimedb_rs_nif", load = load);
