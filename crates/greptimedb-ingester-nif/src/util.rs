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

pub fn terms_to_rows(table_schema: &TableSchema, rows_term: Vec<Term>) -> rustler::NifResult<Rows> {
    let column_schemas = table_schema.columns();
    let mut greptime_rows = Rows::new(column_schemas, rows_term.len(), 1024)
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    for row_term in rows_term {
        let env = row_term.get_env();

        let tags_map: Option<HashMap<String, Term>> =
            if let Some(t) = get_term(row_term, atoms::tags().to_term(env), "tags") {
                t.decode().ok()
            } else {
                None
            };

        let fields_map: Option<HashMap<String, Term>> =
            if let Some(f) = get_term(row_term, atoms::fields().to_term(env), "fields") {
                f.decode().ok()
            } else {
                None
            };

        // Timestamp logic
        let ts_val_term = get_term(row_term, atoms::timestamp().to_term(env), "timestamp")
            .or_else(|| get_term(row_term, atoms::ts().to_term(env), "ts"));

        let mut values = Vec::new();

        for col in column_schemas {
            let val = match col.semantic_type {
                SemanticType::Timestamp => {
                    if let Some(v) = ts_val_term {
                        term_to_value(&v, col.data_type)?
                    } else {
                        Value::Null
                    }
                }
                SemanticType::Tag => {
                    if let Some(map) = &tags_map {
                        if let Some(v) = map.get(&col.name) {
                            term_to_value(v, col.data_type)?
                        } else {
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                }
                SemanticType::Field => {
                    if let Some(map) = &fields_map {
                        if let Some(v) = map.get(&col.name) {
                            term_to_value(v, col.data_type)?
                        } else {
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                }
            };
            values.push(val);
        }

        greptime_rows
            .add_row(Row::from_values(values))
            .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
    }
    Ok(greptime_rows)
}
