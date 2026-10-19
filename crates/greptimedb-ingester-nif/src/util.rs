use std::collections::{BTreeMap, HashSet};

use crate::atoms;
use crate::types;
use greptimedb_ingester::api::v1::column_data_type_extension::TypeExt;
use greptimedb_ingester::api::v1::{
    ColumnDataType, ColumnSchema, Row as ProtoRow, SemanticType, Value as ProtoValue,
};
use greptimedb_ingester::helpers::schema::{field, json2_field, tag, timestamp};
use greptimedb_ingester::helpers::values::{json2_value, none_value};
use greptimedb_ingester::{Row, Rows, TableSchema, Value};
use rustler::{Encoder, Term, TermType};

pub fn terms_to_rows<'a>(
    table_schema: &TableSchema,
    rows_term: Vec<Term<'a>>,
) -> rustler::NifResult<Rows> {
    let column_schemas = table_schema.columns();
    let mut greptime_rows = Rows::new(column_schemas, rows_term.len())
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

/// Whether the column is a JSON2 column (datatype JSON with the native JSON
/// type extension). Only such columns accept structured `JsonValue` values.
pub fn is_json2_column(column: &ColumnSchema) -> bool {
    column.datatype() == ColumnDataType::Json
        && matches!(
            column
                .datatype_extension
                .as_ref()
                .and_then(|ext| ext.type_ext.as_ref()),
            Some(TypeExt::JsonNativeType(_))
        )
}

/// Extracts the JSON document string from a JSON2 field value term.
///
/// Accepted forms:
/// - `nil`/`undefined` -> SQL NULL (`Ok(None)`);
/// - `{json2, JsonBinary}` -> explicit JSON2 marker, used when the table does
///   not exist yet so the column is auto-created with the JSON2 type;
/// - a plain binary/string -> JSON document for a column that is already known
///   to be JSON2 (its schema came from the server).
fn json2_str(term: Term) -> rustler::NifResult<Option<String>> {
    if is_null(term) {
        return Ok(None);
    }
    if let Ok((tag_term, payload)) = term.decode::<(Term, Term)>() {
        if let Ok(tag) = tag_term.decode::<rustler::types::atom::Atom>() {
            if tag == atoms::json2() {
                return payload.decode::<String>().map(Some).map_err(|_| {
                    rustler::Error::RaiseTerm(Box::new(
                        "Invalid json2 value: expected `{json2, JsonBinary}`".to_string(),
                    ))
                });
            }
        }
    }
    term.decode::<String>().map(Some).map_err(|_| {
        rustler::Error::RaiseTerm(Box::new(
            "Invalid json2 value: expected a JSON binary/string or `{json2, JsonBinary}`"
                .to_string(),
        ))
    })
}

/// Encodes a JSON document for a JSON2 column.
///
/// JSON2 values must be sent as structured `JsonValue` messages; the server
/// rejects plain strings for JSON2 columns.
fn json2_proto_value(json: &str) -> rustler::NifResult<ProtoValue> {
    json2_value(json).map_err(|e| {
        rustler::Error::RaiseTerm(Box::new(format!("Invalid json2 value for column: {e}")))
    })
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
    let col_meta: Vec<(SemanticType, Term<'a>, ColumnDataType, bool)> = columns
        .iter()
        .map(|c| {
            (
                SemanticType::try_from(c.semantic_type).unwrap_or(SemanticType::Field),
                c.column_name.encode(env),
                ColumnDataType::try_from(c.datatype).unwrap_or(ColumnDataType::String),
                is_json2_column(c),
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

        for (semantic, key_term, dtype, is_json2) in &col_meta {
            let val_term = match semantic {
                SemanticType::Field => fields_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Tag => tags_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Timestamp => ts_term,
            };

            let val = if let Some(t) = val_term {
                if *is_json2 {
                    match json2_str(t)? {
                        Some(json) => json2_proto_value(&json)?,
                        None => none_value(),
                    }
                } else {
                    types::term_to_proto_value(&t, *dtype)?
                }
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
/// Inference looks at every row in the batch. The first non-null occurrence of
/// an unknown key determines its data type; a later value that does not fit
/// that type is rejected when the rows are encoded, instead of being dropped.
/// NULL values never decide a type, and a key whose values are all NULL does not
/// produce a column at all.
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
        merged.push(inferred_column(&name, dtype));
    }
    for (name, dtype) in new_tags {
        if dtype == ColumnDataType::Json {
            return Err(rustler::Error::RaiseTerm(Box::new(
                "JSON2 columns must be fields, not tags".to_string(),
            )));
        }
        merged.push(tag(&name, dtype));
    }

    Ok(merged)
}

/// Builds the column definition for a field inferred from input rows.
///
/// Inferred JSON is always written as JSON2: it is declared with the native
/// JSON type extension and the `greptime.json2` column options, so an
/// auto-created table gets a real JSON2 column.
fn inferred_column(name: &str, dtype: ColumnDataType) -> ColumnSchema {
    if dtype == ColumnDataType::Json {
        json2_field(name)
    } else {
        field(name, dtype)
    }
}

/// Collects the keys of `map` that are neither in `known` nor already in `out`.
///
/// Keys are visited in sorted order so the resulting column order is
/// deterministic. The first *non-null* occurrence of a key wins, so entries
/// already collected from an earlier row are never overwritten, and a key whose
/// values are all NULL is not collected at all.
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
        if is_null(val) {
            // NULL must not decide the type of an inferred column: defer the
            // inference to the first non-null occurrence, which may be in a
            // later row. If every value is NULL the key is left out entirely,
            // rather than creating an arbitrarily typed column for it.
            continue;
        }
        out.insert(name, infer_dtype(val));
    }
    Ok(())
}

/// Whether `term` stands for a SQL NULL (`nil` or `undefined`).
fn is_null(term: Term) -> bool {
    match term.decode::<rustler::types::atom::Atom>() {
        Ok(atom) => {
            atom == rustler::types::atom::nil() || atom == rustler::types::atom::undefined()
        }
        Err(_) => false,
    }
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
    // `{json2, JsonBinary}` marks a JSON2 column when the schema is inferred
    // (auto-created table).
    if let Ok((tag_term, _)) = term.decode::<(Term, Term)>() {
        if let Ok(tag) = tag_term.decode::<rustler::types::atom::Atom>() {
            if tag == atoms::json2() {
                return ColumnDataType::Json;
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use greptimedb_ingester::api::v1::value::ValueData;
    use greptimedb_ingester::api::v1::{
        json_object, json_value, JsonValue, RowInsertRequest, RowInsertRequests, Rows as ProtoRows,
    };
    use greptimedb_ingester::helpers::values::timestamp_millisecond_value;

    fn object_entries(value: &ProtoValue) -> &[json_object::Entry] {
        let Some(ValueData::JsonValue(value)) = &value.value_data else {
            panic!("expected structured JsonValue, got {:?}", value.value_data);
        };
        let Some(json_value::Value::Object(object)) = &value.value else {
            panic!("expected JSON object, got {:?}", value.value);
        };
        &object.entries
    }

    fn entry<'a>(entries: &'a [json_object::Entry], key: &str) -> &'a JsonValue {
        entries
            .iter()
            .find(|entry| entry.key == key)
            .and_then(|entry| entry.value.as_ref())
            .unwrap_or_else(|| panic!("missing JSON entry `{key}`"))
    }

    #[test]
    fn inferred_json_field_is_json2() {
        let inferred = inferred_column("payload", ColumnDataType::Json);
        assert!(is_json2_column(&inferred));
        assert_eq!(inferred.column_name, "payload");

        // A plain legacy JSON field must not be mistaken for JSON2.
        assert!(!is_json2_column(&field("payload", ColumnDataType::Json)));
        assert!(!is_json2_column(&field("payload", ColumnDataType::String)));
    }

    #[test]
    fn json2_proto_value_encodes_nested_json() {
        let value =
            json2_proto_value(r#"{"name":"alice","nested":{"ok":true},"items":[1,"two",null]}"#)
                .expect("encode JSON2 value");
        let entries = object_entries(&value);
        assert_eq!(entries.len(), 3);

        assert!(matches!(
            entry(entries, "name").value,
            Some(json_value::Value::Str(ref value)) if value == "alice"
        ));
        assert!(matches!(
            entry(entries, "nested").value,
            Some(json_value::Value::Object(ref object))
                if matches!(
                    object.entries.first().and_then(|entry| entry.value.as_ref()),
                    Some(JsonValue {
                        value: Some(json_value::Value::Boolean(true)),
                    })
                )
        ));
        assert!(matches!(
            entry(entries, "items").value,
            Some(json_value::Value::Array(ref array))
                if array.items.len() == 3
                    && matches!(
                        array.items[0].value,
                        Some(json_value::Value::Uint(1))
                    )
                    && matches!(
                        array.items[1].value,
                        Some(json_value::Value::Str(ref value)) if value == "two"
                    )
                    && array.items[2].value.is_none()
        ));
    }

    #[test]
    fn json2_null_becomes_sql_null() {
        let value = json2_proto_value("null").expect("encode JSON2 null");
        assert!(value.value_data.is_none());
    }

    #[test]
    fn json2_row_insert_request_uses_structured_value() {
        let request = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: "json2_test".to_string(),
                rows: Some(ProtoRows {
                    schema: vec![
                        timestamp("ts", ColumnDataType::TimestampMillisecond),
                        inferred_column("payload", ColumnDataType::Json),
                    ],
                    rows: vec![ProtoRow {
                        values: vec![
                            timestamp_millisecond_value(1),
                            json2_proto_value(r#"{"ok":true}"#).expect("encode JSON2 value"),
                        ],
                    }],
                }),
            }],
        };

        let rows = request.inserts[0].rows.as_ref().expect("row insert rows");
        assert!(is_json2_column(&rows.schema[1]));
        assert!(matches!(
            rows.rows[0].values[1].value_data.as_ref(),
            Some(ValueData::JsonValue(_))
        ));
    }

    #[test]
    fn json2_rejects_invalid_json_and_non_object_top_level_values() {
        for invalid in [
            "",
            "{",
            r#"{"a":1} trailing"#,
            "[]",
            "1",
            "true",
            "\"text\"",
        ] {
            assert!(
                json2_proto_value(invalid).is_err(),
                "expected `{invalid}` to be rejected"
            );
        }
    }
}
