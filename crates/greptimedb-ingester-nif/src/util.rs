use crate::atoms;
use greptimedb_ingester::api::v1::{ColumnDataType, SemanticType};
use greptimedb_ingester::{Row, Rows, TableSchema, Value};
use rustler::{Encoder, Term, TermType};
use std::collections::HashMap;

fn get_term<'a>(map: Term<'a>, atom_key: Term<'a>, str_key: &str) -> Option<Term<'a>> {
    if let Ok(val) = map.map_get(atom_key) {
        return Some(val);
    }
    let env = map.get_env();
    let str_term = str_key.encode(env);
    map.map_get(str_term).ok()
}

fn infer_type(val: &Term) -> ColumnDataType {
    match val.get_type() {
        TermType::Integer => ColumnDataType::Int64,
        TermType::Float => ColumnDataType::Float64,
        TermType::Binary => ColumnDataType::String,
        TermType::Atom => {
            if let Ok(_b) = val.decode::<bool>() {
                ColumnDataType::Boolean
            } else {
                ColumnDataType::String
            }
        }
        _ => ColumnDataType::String,
    }
}

fn term_to_value(val: &Term, dtype: ColumnDataType) -> rustler::NifResult<Value> {
    match dtype {
        ColumnDataType::Int64 => {
            if let Ok(i) = val.decode::<i64>() {
                Ok(Value::Int64(i))
            } else {
                Ok(Value::Null)
            }
        }
        ColumnDataType::Float64 => {
            if let Ok(f) = val.decode::<f64>() {
                Ok(Value::Float64(f))
            } else {
                Ok(Value::Null)
            }
        }
        ColumnDataType::String => {
            if let Ok(s) = val.decode::<String>() {
                Ok(Value::String(s))
            } else {
                Ok(Value::Null)
            }
        }
        ColumnDataType::Boolean => {
            if let Ok(b) = val.decode::<bool>() {
                Ok(Value::Boolean(b))
            } else {
                Ok(Value::Null)
            }
        }
        ColumnDataType::TimestampMillisecond => {
            if let Ok(i) = val.decode::<i64>() {
                Ok(Value::TimestampMillisecond(i))
            } else {
                Ok(Value::Null)
            }
        }
        // Add other types as needed
        _ => Ok(Value::Null),
    }
}

pub fn terms_to_table_schema(table: &str, first_row: Term) -> rustler::NifResult<TableSchema> {
    let env = first_row.get_env();
    let mut table_template = TableSchema::builder()
        .name(table)
        .build()
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    // 1. Timestamp (Always "ts" for now, mapped from "timestamp" or "ts" key)
    table_template = table_template.add_timestamp("ts", ColumnDataType::TimestampMillisecond);

    // 2. Tags
    if let Some(tags_term) = get_term(first_row, atoms::tags().to_term(env), "tags") {
        let tags_map: HashMap<String, Term> = tags_term.decode()?;
        let mut sorted_keys: Vec<String> = tags_map.keys().cloned().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            let val = tags_map.get(&key).unwrap();
            let dtype = infer_type(val);
            table_template = table_template.add_tag(key, dtype);
        }
    }

    // 3. Fields
    if let Some(fields_term) = get_term(first_row, atoms::fields().to_term(env), "fields") {
        let fields_map: HashMap<String, Term> = fields_term.decode()?;
        let mut sorted_keys: Vec<String> = fields_map.keys().cloned().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            let val = fields_map.get(&key).unwrap();
            let dtype = infer_type(val);
            table_template = table_template.add_field(key, dtype);
        }
    }

    Ok(table_template)
}

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

    // Pre-compute static atoms and binary keys
    let atom_fields = atoms::fields().to_term(env);
    let bin_fields = "fields".encode(env);
    let atom_tags = atoms::tags().to_term(env);
    let bin_tags = "tags".encode(env);
    let atom_timestamp = atoms::timestamp().to_term(env);
    let bin_timestamp = "timestamp".encode(env);
    let atom_ts = atoms::ts().to_term(env);
    let bin_ts = "ts".encode(env);

    for row_term in rows_term {
        // Retrieve sub-maps directly from the row term (atom or binary key)
        let fields_term = row_term.map_get(atom_fields).ok().or_else(|| row_term.map_get(bin_fields).ok());
        let tags_term = row_term.map_get(atom_tags).ok().or_else(|| row_term.map_get(bin_tags).ok());

        // Timestamp can be under "timestamp" or "ts" (atom or binary)
        let ts_term = row_term.map_get(atom_timestamp).ok()
            .or_else(|| row_term.map_get(bin_timestamp).ok())
            .or_else(|| row_term.map_get(atom_ts).ok())
            .or_else(|| row_term.map_get(bin_ts).ok());

        let mut values = Vec::with_capacity(col_meta.len());

        for (semantic, key_term, dtype) in &col_meta {
            let val_term = match semantic {
                SemanticType::Field => fields_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Tag => tags_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Timestamp => ts_term,
            };

            let val = if let Some(t) = val_term {
                term_to_value(&t, *dtype)?
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
