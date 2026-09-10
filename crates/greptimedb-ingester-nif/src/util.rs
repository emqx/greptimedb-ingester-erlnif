use std::collections::{BTreeMap, HashSet};

use crate::atoms;
use crate::types;
use greptimedb_ingester::api::v1::{ColumnDataType, ColumnSchema, Row as ProtoRow, SemanticType};
use greptimedb_ingester::helpers::schema::{field, tag, timestamp};
use greptimedb_ingester::helpers::values::none_value;
use greptimedb_ingester::{Row, Rows, TableSchema, Value};
use rustler::{Encoder, Term, TermType};

pub fn terms_to_rows<'a>(
    table_schema: &TableSchema,
    rows_term: Vec<Term<'a>>,
) -> rustler::NifResult<Rows> {
    let column_schemas = table_schema.columns();
    let mut greptime_rows = Rows::new(column_schemas, rows_term.len(), 1024)
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    if rows_term.is_empty() {
        return Ok(greptime_rows);
    }

    let env = rows_term[0].get_env();

    // Pre-compute keys and metadata for columns to avoid repetitive encoding/decoding
    let col_meta: Vec<(SemanticType, Term<'a>, ColumnDataType)> = column_schemas
        .iter()
        .map(|c| (c.semantic_type, c.name.encode(env), c.data_type))
        .collect();

    // Pre-compute static atom keys only
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);
    let atom_timestamp = atoms::timestamp().to_term(env);
    let atom_ts = atoms::ts().to_term(env);

    for row_term in rows_term {
        // Retrieve sub-maps directly from the row term (atom keys only)
        let fields_term = row_term.map_get(atom_fields).ok();
        let tags_term = row_term.map_get(atom_tags).ok();

        // Timestamp can be under "timestamp" or "ts" (atom keys only)
        let ts_term = row_term
            .map_get(atom_timestamp)
            .ok()
            .or_else(|| row_term.map_get(atom_ts).ok());

        let mut values = Vec::with_capacity(col_meta.len());

        for (semantic, key_term, dtype) in &col_meta {
            let val_term = match semantic {
                SemanticType::Field => fields_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Tag => tags_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Timestamp => ts_term,
            };

            let val = if let Some(t) = val_term {
                types::term_to_value(&t, *dtype)?
            } else {
                Value::Null
            };
            values.push(val);
        }

        greptime_rows
            .add_row(Row::from_values(values))
            .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
    }
    Ok(greptime_rows)
}

/// Encodes `rows_term` against an explicit column list.
///
/// This is the single row encoder used by both the "table exists" path (with the
/// merged schema) and the "table does not exist" path (with a locally inferred
/// schema), so both encode rows the same way.
pub fn terms_to_proto_rows_with_columns<'a>(
    columns: &[ColumnSchema],
    rows_term: Vec<Term<'a>>,
) -> rustler::NifResult<Vec<ProtoRow>> {
    if rows_term.is_empty() {
        return Ok(Vec::new());
    }

    let env = rows_term[0].get_env();

    // Pre-compute keys and metadata for columns to avoid repetitive encoding/decoding
    let col_meta: Vec<(SemanticType, Term<'a>, ColumnDataType)> = columns
        .iter()
        .map(|c| {
            (
                SemanticType::try_from(c.semantic_type).unwrap_or(SemanticType::Field),
                c.column_name.encode(env),
                ColumnDataType::try_from(c.datatype).unwrap_or(ColumnDataType::String),
            )
        })
        .collect();

    // Pre-compute static atom keys only
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);
    let atom_timestamp = atoms::timestamp().to_term(env);
    let atom_ts = atoms::ts().to_term(env);

    let mut rows = Vec::with_capacity(rows_term.len());

    for row_term in rows_term {
        // Retrieve sub-maps directly from the row term (atom keys only)
        let fields_term = row_term.map_get(atom_fields).ok();
        let tags_term = row_term.map_get(atom_tags).ok();

        // Timestamp can be under "timestamp" or "ts" (atom keys only)
        let ts_term = row_term
            .map_get(atom_timestamp)
            .ok()
            .or_else(|| row_term.map_get(atom_ts).ok());

        let mut values = Vec::with_capacity(col_meta.len());

        for (semantic, key_term, dtype) in &col_meta {
            let val_term = match semantic {
                SemanticType::Field => fields_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Tag => tags_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Timestamp => ts_term,
            };

            let val = if let Some(t) = val_term {
                types::term_to_proto_value(&t, *dtype)?
            } else {
                none_value()
            };
            values.push(val);
        }
        rows.push(ProtoRow { values });
    }
    Ok(rows)
}

/// Merges the columns known by the server with the columns inferred from the
/// input rows.
///
/// The server's columns are kept verbatim (name, data type and semantic type),
/// so that rows are always encoded with the types the table actually has.
/// Unknown keys found under `fields` are appended as `FIELD` columns; unknown
/// keys found only under `tags` are appended as `TAG` columns.
///
/// If the same unknown key appears under both `fields` and `tags`, `fields`
/// wins: GreptimeDB rejects requests carrying the same column name twice
/// (`Duplicated column name in gRPC requests`), so exactly one column may be
/// produced per key.
///
/// Inference looks at every row in the batch. The first occurrence of an
/// unknown key determines its data type; a later value that does not fit that
/// type is rejected when the rows are encoded, instead of being dropped.
pub fn merge_inferred_columns<'a>(
    server_columns: Vec<ColumnSchema>,
    rows_term: &[Term<'a>],
) -> rustler::NifResult<Vec<ColumnSchema>> {
    let mut merged = server_columns;
    if rows_term.is_empty() {
        return Ok(merged);
    }

    let env = rows_term[0].get_env();
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);

    let known: HashSet<String> = merged.iter().map(|c| c.column_name.clone()).collect();

    let mut new_fields: BTreeMap<String, ColumnDataType> = BTreeMap::new();
    let mut new_tags: BTreeMap<String, ColumnDataType> = BTreeMap::new();

    for row_term in rows_term {
        if let Ok(map) = row_term.map_get(atom_fields) {
            collect_unknown_columns(map, &known, &mut new_fields)?;
        }
    }
    for row_term in rows_term {
        if let Ok(map) = row_term.map_get(atom_tags) {
            collect_unknown_columns(map, &known, &mut new_tags)?;
        }
    }
    // `fields` wins over `tags`, see the doc comment above.
    for name in new_fields.keys() {
        new_tags.remove(name);
    }

    for (name, dtype) in new_fields {
        merged.push(field(&name, dtype));
    }
    for (name, dtype) in new_tags {
        merged.push(tag(&name, dtype));
    }

    Ok(merged)
}

/// Collects the keys of `map` that are neither in `known` nor already in `out`.
///
/// Keys are visited in sorted order so the resulting column order is
/// deterministic. The first occurrence of a key wins, so entries already
/// collected from an earlier row are never overwritten.
fn collect_unknown_columns(
    map: Term,
    known: &HashSet<String>,
    out: &mut BTreeMap<String, ColumnDataType>,
) -> rustler::NifResult<()> {
    let mut keys: Vec<Term> = map
        .decode::<rustler::MapIterator>()
        .map_err(|_| rustler::Error::BadArg)?
        .map(|(k, _)| k)
        .collect();
    keys.sort();

    for key in keys {
        let name = term_to_string(key)?;
        if known.contains(&name) || out.contains_key(&name) {
            continue;
        }
        let val = map.map_get(key)?;
        out.insert(name, infer_dtype(val));
    }
    Ok(())
}

pub fn terms_to_schema_and_rows<'a>(
    rows_term: Vec<Term<'a>>,
    ts_column: &str,
) -> rustler::NifResult<(Vec<ColumnSchema>, Vec<ProtoRow>)> {
    if rows_term.is_empty() {
        return Ok((vec![], vec![]));
    }

    let env = rows_term[0].get_env();
    let first_row = rows_term[0];

    // Pre-compute static atom keys only
    let atom_timestamp = atoms::timestamp().to_term(env);
    let atom_ts = atoms::ts().to_term(env);

    // The timestamp column comes first; every other column is inferred from the
    // rows themselves (tags and fields, across all rows).
    let mut schema = Vec::new();

    let ts_term = first_row
        .map_get(atom_timestamp)
        .ok()
        .or_else(|| first_row.map_get(atom_ts).ok());

    if ts_term.is_some() {
        let ts_name = if ts_column.is_empty() {
            "ts"
        } else {
            ts_column
        };
        schema.push(timestamp(ts_name, ColumnDataType::TimestampMillisecond));
    }

    let schema = merge_inferred_columns(schema, &rows_term)?;
    let rows = terms_to_proto_rows_with_columns(&schema, rows_term)?;

    Ok((schema, rows))
}

fn infer_dtype(term: Term) -> ColumnDataType {
    match term.get_type() {
        TermType::Atom => {
            if term.decode::<bool>().is_ok() {
                ColumnDataType::Boolean
            } else {
                ColumnDataType::String
            }
        }
        TermType::Binary => ColumnDataType::String,
        TermType::Integer => {
            // Aggressively default to Int64 to avoid overflow issues with schema inference
            // unless it fits in i64. If it's too big for i64 (u64), use Uint64.
            if term.decode::<i64>().is_ok() {
                ColumnDataType::Int64
            } else if term.decode::<u64>().is_ok() {
                ColumnDataType::Uint64
            } else {
                ColumnDataType::Float64
            }
        }
        TermType::Float => ColumnDataType::Float64,
        _ => ColumnDataType::String,
    }
}

fn term_to_string(term: Term) -> rustler::NifResult<String> {
    if let Ok(s) = term.decode::<String>() {
        Ok(s)
    } else if let Ok(a) = term.atom_to_string() {
        Ok(a)
    } else {
        term.decode::<String>()
    }
}
