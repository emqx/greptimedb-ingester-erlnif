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

pub fn terms_to_proto_rows_using_schema<'a>(
    table_schema: &TableSchema,
    rows_term: Vec<Term<'a>>,
) -> rustler::NifResult<Vec<ProtoRow>> {
    if rows_term.is_empty() {
        return Ok(Vec::new());
    }

    let env = rows_term[0].get_env();
    let column_schemas = table_schema.columns();

    // Pre-compute keys and metadata for columns
    let col_meta: Vec<(SemanticType, Term<'a>, ColumnDataType)> = column_schemas
        .iter()
        .map(|c| (c.semantic_type, c.name.encode(env), c.data_type))
        .collect();

    // Pre-compute static atom keys only
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);
    let atom_timestamp = atoms::timestamp().to_term(env);
    let atom_ts = atoms::ts().to_term(env);

    let mut rows = Vec::with_capacity(rows_term.len());

    for row_term in rows_term {
        let fields_term = row_term.map_get(atom_fields).ok();
        let tags_term = row_term.map_get(atom_tags).ok();
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

pub fn terms_to_schema_and_rows<'a>(
    rows_term: Vec<Term<'a>>,
) -> rustler::NifResult<(Vec<ColumnSchema>, Vec<ProtoRow>)> {
    if rows_term.is_empty() {
        return Ok((vec![], vec![]));
    }

    let env = rows_term[0].get_env();
    let first_row = rows_term[0];

    // Pre-compute static atom keys only
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);
    let atom_timestamp = atoms::timestamp().to_term(env);
    let atom_ts = atoms::ts().to_term(env);

    let fields_term = first_row.map_get(atom_fields).ok();
    let tags_term = first_row.map_get(atom_tags).ok();

    // --- Infer Schema ---
    let mut schema = Vec::new();

    // 1. Tags
    if let Some(map) = tags_term {
        let mut keys: Vec<Term> = match map.decode::<rustler::MapIterator>() {
            Ok(iter) => iter.map(|(k, _)| k).collect(),
            Err(_) => return Err(rustler::Error::BadArg),
        };
        keys.sort();

        for key in keys {
            let val = map.map_get(key)?;
            let dtype = infer_dtype(val);
            let name = term_to_string(key)?;
            schema.push(tag(&name, dtype));
        }
    }

    // 2. Fields
    if let Some(map) = fields_term {
        let mut keys: Vec<Term> = match map.decode::<rustler::MapIterator>() {
            Ok(iter) => iter.map(|(k, _)| k).collect(),
            Err(_) => return Err(rustler::Error::BadArg),
        };
        keys.sort();

        for key in keys {
            let val = map.map_get(key)?;
            let dtype = infer_dtype(val);
            let name = term_to_string(key)?;
            schema.push(field(&name, dtype));
        }
    }

    // 3. Timestamp
    let ts_term = first_row
        .map_get(atom_timestamp)
        .ok()
        .or_else(|| first_row.map_get(atom_ts).ok());

    if ts_term.is_some() {
        schema.push(timestamp("ts", ColumnDataType::TimestampMillisecond));
    }

    // --- Build Rows ---
    struct ColMeta<'a> {
        semantic: SemanticType,
        key_term: Term<'a>,
        dtype: ColumnDataType,
    }

    let mut col_meta_list = Vec::with_capacity(schema.len());
    for col in &schema {
        let key_term = col.column_name.encode(env);
        let semantic = SemanticType::try_from(col.semantic_type).unwrap_or(SemanticType::Field);
        let dtype = ColumnDataType::try_from(col.datatype).unwrap_or(ColumnDataType::String);

        col_meta_list.push(ColMeta {
            semantic,
            key_term,
            dtype,
        });
    }

    let mut rows = Vec::with_capacity(rows_term.len());

    use greptimedb_ingester::helpers::values::none_value;

    for row_term in rows_term {
        let fields_map = row_term.map_get(atom_fields).ok();
        let tags_map = row_term.map_get(atom_tags).ok();

        let row_ts_term = row_term
            .map_get(atom_timestamp)
            .ok()
            .or_else(|| row_term.map_get(atom_ts).ok());

        let mut values = Vec::with_capacity(schema.len());

        for meta in &col_meta_list {
            let val_term = match meta.semantic {
                SemanticType::Tag => find_value_in_map(tags_map, meta.key_term),
                SemanticType::Field => find_value_in_map(fields_map, meta.key_term),
                SemanticType::Timestamp => row_ts_term,
            };

            let val = if let Some(t) = val_term {
                types::term_to_proto_value(&t, meta.dtype)?
            } else {
                none_value()
            };
            values.push(val);
        }
        rows.push(ProtoRow { values });
    }

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

fn find_value_in_map<'a>(map: Option<Term<'a>>, key_str_term: Term<'a>) -> Option<Term<'a>> {
    map.and_then(|m| m.map_get(key_str_term).ok())
}
