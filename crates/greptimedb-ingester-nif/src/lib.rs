use greptime_proto::v1::auth_header::AuthScheme;
use greptime_proto::v1::Basic;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::{
    BulkInserter, BulkStreamWriter, BulkWriteOptions, CompressionType, TableSchema,
};
use lazy_static::lazy_static;
use rustler::{Encoder, Env, NifResult, ResourceArc, Term};
use std::time::Duration;
use tokio::runtime::Runtime;

pub mod atoms;
mod util;

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

pub struct GreptimeResource {
    pub db: Database,
    pub client: Client,
    pub auth: Option<AuthScheme>,
}

pub struct StreamWriterResource {
    pub writer: tokio::sync::Mutex<Option<BulkStreamWriter>>,
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

    let result = RUNTIME.block_on(async {
        use futures::StreamExt;
        match db.query(&sql).await {
            Ok(mut stream) => {
                let mut rows = Vec::new();
                while let Some(batch_res) = stream.next().await {
                    match batch_res {
                        Ok(batch) => {
                            rows.push(format!("{batch:?}"));
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(rows)
            }
            Err(e) => Err(e),
        }
    });

    match result {
        Ok(rows) => Ok((atoms::ok(), rows).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
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

    // 1. Infer Schema from the first row
    let first_row_term = rows_term[0];

    let table_template = util::terms_to_table_schema(&table, first_row_term)?;

    // 2. Construct Rows
    let greptime_rows = util::terms_to_rows(&table_template, rows_term)?;

    let result: Result<u32, String> = RUNTIME.block_on(async {
        let mut bulk_inserter = BulkInserter::new(resource.client.clone(), resource.db.dbname());
        if let Some(auth) = &resource.auth {
            bulk_inserter.set_auth(auth.clone());
        }

        let mut writer = bulk_inserter
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

        let response = writer
            .write_rows(greptime_rows)
            .await
            .map_err(|e| e.to_string())?;
        writer.finish().await.map_err(|e| e.to_string())?;

        Ok(response.affected_rows() as u32)
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
    first_row: Term<'a>,
) -> NifResult<Term<'a>> {
    // Infer schema
    let table_template = util::terms_to_table_schema(&table, first_row)?;
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
            writer: tokio::sync::Mutex::new(Some(writer)),
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
        if let Some(writer) = writer_guard.as_mut() {
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
        if let Some(writer) = writer_guard.take() {
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
