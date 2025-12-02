use greptimedb_ingester::api::v1::{ColumnDataType, SemanticType};
use greptimedb_ingester::{Row, Rows, TableSchema, Value};
use rustler::{Term, TermType};
use std::collections::HashMap;

pub fn terms_to_table_schema(
    table: &str,
    first_row: &HashMap<String, Term>,
) -> rustler::NifResult<TableSchema> {
    let mut table_template = TableSchema::builder()
        .name(table)
        .build()
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    let mut sorted_keys: Vec<String> = first_row.keys().cloned().collect();
    sorted_keys.sort();

    for key in &sorted_keys {
        let val = first_row.get(key).unwrap();
        let (dtype, semantic_type) = match val.get_type() {
            TermType::Integer => (ColumnDataType::Int64, SemanticType::Field),
            TermType::Float => (ColumnDataType::Float64, SemanticType::Field),
            TermType::Binary => (ColumnDataType::String, SemanticType::Tag),
            TermType::Atom => {
                if let Ok(_b) = val.decode::<bool>() {
                    (ColumnDataType::Boolean, SemanticType::Field)
                } else {
                    (ColumnDataType::String, SemanticType::Tag)
                }
            }
            _ => (ColumnDataType::String, SemanticType::Field),
        };

        let (final_dtype, final_semantic) = if key == "ts" || key == "timestamp" {
            (
                ColumnDataType::TimestampMillisecond,
                SemanticType::Timestamp,
            )
        } else {
            (dtype, semantic_type)
        };

        match final_semantic {
            SemanticType::Timestamp => {
                table_template = table_template.add_timestamp(key, final_dtype);
            }
            SemanticType::Tag => {
                table_template = table_template.add_tag(key, final_dtype);
            }
            SemanticType::Field => {
                table_template = table_template.add_field(key, final_dtype);
            }
        }
    }
    Ok(table_template)
}

pub fn terms_to_rows(table_schema: &TableSchema, rows_term: Vec<Term>) -> rustler::NifResult<Rows> {
    let column_schemas = table_schema.columns();
    let mut greptime_rows = Rows::new(column_schemas, rows_term.len(), 1024)
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    let column_names: Vec<String> = column_schemas.iter().map(|c| c.name.clone()).collect();

    for row_term in rows_term {
        let row_map: HashMap<String, Term> = row_term.decode()?;
        let mut values = Vec::new();

        for col_name in &column_names {
            if let Some(val_term) = row_map.get(col_name) {
                let val = if let Ok(i) = val_term.decode::<i64>() {
                    if col_name == "ts" || col_name == "timestamp" {
                        Value::TimestampMillisecond(i)
                    } else {
                        Value::Int64(i)
                    }
                } else if let Ok(f) = val_term.decode::<f64>() {
                    Value::Float64(f)
                } else if let Ok(s) = val_term.decode::<String>() {
                    Value::String(s)
                } else if let Ok(b) = val_term.decode::<bool>() {
                    Value::Boolean(b)
                } else {
                    Value::Null
                };
                values.push(val);
            } else {
                values.push(Value::Null);
            }
        }
        greptime_rows
            .add_row(Row::from_values(values))
            .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
    }
    Ok(greptime_rows)
}
