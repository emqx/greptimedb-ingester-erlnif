use std::collections::{BTreeMap, HashMap, HashSet};

use crate::atoms;
use crate::types;
use greptimedb_ingester::api::v1::column_data_type_extension::TypeExt;
use greptimedb_ingester::api::v1::value::ValueData;
use greptimedb_ingester::api::v1::{
    ColumnDataType, ColumnSchema, Row as ProtoRow, SemanticType, Value as ProtoValue,
};
use greptimedb_ingester::helpers::schema::{field, json2_field, tag, timestamp};
use greptimedb_ingester::helpers::values::{json2_value, none_value};
use greptimedb_ingester::{Row, Rows, TableSchema, Value};
use rustler::{Encoder, Term, TermType};

type ColumnMeta<'a> = (
    SemanticType,
    Term<'a>,
    ColumnDataType,
    bool,
    Option<Result<ProtoValue, String>>,
);

#[derive(Debug, Clone)]
pub enum ColumnDefault {
    Null,
    Value(String),
}

pub struct RowTerms<'a> {
    fields: Option<Term<'a>>,
    tags: Option<Term<'a>>,
    timestamp: Option<Term<'a>>,
}

pub struct KnownBatchPlan<'a> {
    pub row_terms: Vec<RowTerms<'a>>,
    pub row_masks: Vec<u64>,
    pub words: usize,
    pub all_same: bool,
    pub column_indices: Vec<usize>,
}

type PlanColumnMeta<'a> = (
    usize,
    SemanticType,
    Term<'a>,
    ColumnDataType,
    bool,
    Option<Result<ProtoValue, String>>,
);

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

/// Converts a column default from `information_schema.columns` into a concrete
/// value.
///
/// Defaults are only needed when a mixed batch has rows that provide a column
/// and rows that omit it. STRING and JSON defaults are already stored as raw
/// displayed values; temporal defaults are displayed as ISO strings.
fn parse_column_default(
    default: &ColumnDefault,
    column: &ColumnSchema,
) -> Result<ProtoValue, String> {
    match default {
        ColumnDefault::Null => Ok(none_value()),
        ColumnDefault::Value(value) => parse_default_value(value, column),
    }
}

fn parse_default_value(default: &str, column: &ColumnSchema) -> Result<ProtoValue, String> {
    if is_json2_column(column) {
        return Err(format!(
            "JSON2 column `{}` has a default that cannot be applied to an omitted value in a mixed batch",
            column.column_name
        ));
    }

    let dtype = ColumnDataType::try_from(column.datatype)
        .map_err(|_| format!("Unknown type for column `{}`", column.column_name))?;
    let name = column.column_name.as_str();
    let value_data = match dtype {
        ColumnDataType::String | ColumnDataType::Json => {
            ValueData::StringValue(default.to_string())
        }
        ColumnDataType::Boolean => match default.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => ValueData::BoolValue(true),
            "false" | "0" => ValueData::BoolValue(false),
            _ => return Err(unsupported_default(dtype, name, default)),
        },
        ColumnDataType::Int8 => ValueData::I8Value(i32::from(parse_scalar_default::<i8>(
            default, name, "Int8",
        )?)),
        ColumnDataType::Int16 => ValueData::I16Value(i32::from(parse_scalar_default::<i16>(
            default, name, "Int16",
        )?)),
        ColumnDataType::Int32 => {
            ValueData::I32Value(parse_scalar_default::<i32>(default, name, "Int32")?)
        }
        ColumnDataType::Int64 => {
            ValueData::I64Value(parse_scalar_default::<i64>(default, name, "Int64")?)
        }
        ColumnDataType::Uint8 => ValueData::U8Value(u32::from(parse_scalar_default::<u8>(
            default, name, "UInt8",
        )?)),
        ColumnDataType::Uint16 => ValueData::U16Value(u32::from(parse_scalar_default::<u16>(
            default, name, "UInt16",
        )?)),
        ColumnDataType::Uint32 => {
            ValueData::U32Value(parse_scalar_default::<u32>(default, name, "UInt32")?)
        }
        ColumnDataType::Uint64 => {
            ValueData::U64Value(parse_scalar_default::<u64>(default, name, "UInt64")?)
        }
        ColumnDataType::Float32 => {
            ValueData::F32Value(parse_scalar_default::<f32>(default, name, "Float32")?)
        }
        ColumnDataType::Float64 => {
            ValueData::F64Value(parse_scalar_default::<f64>(default, name, "Float64")?)
        }
        ColumnDataType::Date => ValueData::DateValue(parse_date_default(default, name)?),
        ColumnDataType::Datetime => ValueData::DatetimeValue(parse_timestamp_default(
            default,
            name,
            "millisecond",
            "Datetime",
        )?),
        ColumnDataType::TimestampSecond => ValueData::TimestampSecondValue(
            parse_timestamp_default(default, name, "second", "TimestampSecond")?,
        ),
        ColumnDataType::TimestampMillisecond => ValueData::TimestampMillisecondValue(
            parse_timestamp_default(default, name, "millisecond", "TimestampMillisecond")?,
        ),
        ColumnDataType::TimestampMicrosecond => ValueData::TimestampMicrosecondValue(
            parse_timestamp_default(default, name, "microsecond", "TimestampMicrosecond")?,
        ),
        ColumnDataType::TimestampNanosecond => ValueData::TimestampNanosecondValue(
            parse_timestamp_default(default, name, "nanosecond", "TimestampNanosecond")?,
        ),
        _ => return Err(unsupported_default(dtype, name, default)),
    };
    Ok(ProtoValue {
        value_data: Some(value_data),
    })
}

fn parse_date_default(default: &str, column: &str) -> Result<i32, String> {
    let value = default.trim();
    if let Ok(days) = value.parse::<i32>() {
        return Ok(days);
    }
    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| format!("Unsupported default for Date column `{column}`: {default}"))?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid Unix epoch");
    Ok(date.signed_duration_since(epoch).num_days() as i32)
}

fn parse_timestamp_default(
    default: &str,
    column: &str,
    unit: &str,
    type_name: &str,
) -> Result<i64, String> {
    let value = default.trim();
    if let Ok(value) = value.parse::<i64>() {
        return Ok(value);
    }
    let dt = parse_datetime_default(value, column, type_name)?;
    match unit {
        "second" => Ok(dt.timestamp()),
        "millisecond" => Ok(dt.timestamp_millis()),
        "microsecond" => Ok(dt.timestamp_micros()),
        "nanosecond" => dt.timestamp_nanos_opt().ok_or_else(|| {
            format!("Timestamp default out of range for column `{column}`: {default}")
        }),
        _ => unreachable!("unsupported timestamp unit"),
    }
}

fn parse_datetime_default(
    value: &str,
    column: &str,
    type_name: &str,
) -> Result<chrono::DateTime<chrono::FixedOffset>, String> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(dt);
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f%z", "%Y-%m-%dT%H:%M:%S%.f%z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(value, format) {
            return Ok(dt);
        }
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(value, format) {
            return Ok(chrono::DateTime::from_naive_utc_and_offset(
                naive,
                chrono::FixedOffset::east_opt(0).expect("valid UTC offset"),
            ));
        }
    }
    Err(format!(
        "Unsupported default for {type_name} column `{column}`: {value}"
    ))
}

fn parse_scalar_default<T>(default: &str, column: &str, type_name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    default
        .trim()
        .parse::<T>()
        .map_err(|_| format!("Unsupported default for {type_name} column `{column}`: {default}"))
}

fn unsupported_default(dtype: ColumnDataType, column: &str, default: &str) -> String {
    format!("Unsupported default for {dtype:?} column `{column}`: {default}")
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
    terms_to_proto_rows_with_columns_and_defaults(columns, rows_term, &HashMap::new())
}

pub fn terms_to_proto_rows_with_columns_and_defaults<'a>(
    columns: &[ColumnSchema],
    rows_term: Vec<Term<'a>>,
    defaults: &HashMap<String, ColumnDefault>,
) -> rustler::NifResult<Vec<ProtoRow>> {
    if rows_term.is_empty() {
        return Ok(Vec::new());
    }

    let env = rows_term[0].get_env();

    // Pre-compute keys and metadata for columns to avoid repetitive encoding/decoding
    let col_meta: Vec<ColumnMeta<'a>> = columns
        .iter()
        .map(|c| {
            (
                SemanticType::try_from(c.semantic_type).unwrap_or(SemanticType::Field),
                c.column_name.encode(env),
                ColumnDataType::try_from(c.datatype).unwrap_or(ColumnDataType::String),
                is_json2_column(c),
                defaults
                    .get(&c.column_name)
                    .map(|default| parse_column_default(default, c)),
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

        for (semantic, key_term, dtype, is_json2, default) in &col_meta {
            let val_term = match semantic {
                SemanticType::Field => fields_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Tag => tags_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Timestamp => ts_term,
            };

            let val = if let Some(t) = val_term {
                encode_term_value(t, *dtype, *is_json2)?
            } else if let Some(default) = default {
                match default {
                    Ok(value) => value.clone(),
                    Err(reason) => {
                        return Err(rustler::Error::RaiseTerm(Box::new(reason.clone())));
                    }
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

fn encode_term_value(
    value: Term,
    dtype: ColumnDataType,
    is_json2: bool,
) -> rustler::NifResult<ProtoValue> {
    if is_json2 {
        match json2_str(value)? {
            Some(json) => json2_proto_value(&json),
            None => Ok(none_value()),
        }
    } else {
        types::term_to_proto_value(&value, dtype)
    }
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

/// Collects the column names provided by at least one input row.
pub fn collect_present_columns<'a>(rows_term: &[Term<'a>]) -> rustler::NifResult<HashSet<String>> {
    if rows_term.is_empty() {
        return Ok(HashSet::new());
    }

    let env = rows_term[0].get_env();
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);
    let mut present = HashSet::new();

    for row_term in rows_term {
        present.extend(row_present_columns(*row_term, atom_fields, atom_tags)?);
    }

    Ok(present)
}

/// Returns true when every row provides exactly the same column names.
pub fn rows_have_same_presence<'a>(rows_term: &[Term<'a>]) -> rustler::NifResult<bool> {
    if rows_term.len() <= 1 {
        return Ok(true);
    }

    let env = rows_term[0].get_env();
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);
    let first = row_present_columns(rows_term[0], atom_fields, atom_tags)?;

    for row_term in &rows_term[1..] {
        if row_present_columns(*row_term, atom_fields, atom_tags)? != first {
            return Ok(false);
        }
    }

    Ok(true)
}

fn row_present_columns<'a>(
    row_term: Term<'a>,
    atom_fields: Term<'a>,
    atom_tags: Term<'a>,
) -> rustler::NifResult<HashSet<String>> {
    let mut present = HashSet::new();
    for map_key in [atom_fields, atom_tags] {
        if let Ok(map) = row_term.map_get(map_key) {
            for (key, _) in map
                .decode::<rustler::MapIterator>()
                .map_err(|_| rustler::Error::BadArg)?
            {
                present.insert(term_to_string(key)?);
            }
        }
    }
    Ok(present)
}

/// Builds a fast-path plan when every input key is already a server column.
///
/// The plan caches row terms, records column presence as a compact bitset, and
/// computes union/uniformity in one pass. Unknown keys return `None` so the
/// caller can use the inference-capable slow path.
pub fn analyze_known_columns<'a>(
    columns: &[ColumnSchema],
    rows_term: &[Term<'a>],
) -> rustler::NifResult<Option<KnownBatchPlan<'a>>> {
    if rows_term.is_empty() {
        return Ok(Some(KnownBatchPlan {
            row_terms: Vec::new(),
            row_masks: Vec::new(),
            words: 0,
            all_same: true,
            column_indices: Vec::new(),
        }));
    }

    let env = rows_term[0].get_env();
    let atom_fields = atoms::fields().to_term(env);
    let atom_tags = atoms::tags().to_term(env);
    let atom_timestamp = atoms::timestamp().to_term(env);
    let atom_ts = atoms::ts().to_term(env);
    let name_to_index: HashMap<Vec<u8>, usize> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.column_name.as_bytes().to_vec(), index))
        .collect();
    let words = columns.len().div_ceil(64);
    let mut row_masks = vec![0u64; rows_term.len() * words];
    let mut row_terms = Vec::with_capacity(rows_term.len());

    for (row_index, row_term) in rows_term.iter().enumerate() {
        let fields = row_term.map_get(atom_fields).ok();
        let tags = row_term.map_get(atom_tags).ok();
        let timestamp = row_term
            .map_get(atom_timestamp)
            .ok()
            .or_else(|| row_term.map_get(atom_ts).ok());

        for map in [fields, tags].into_iter().flatten() {
            for (key, _) in map
                .decode::<rustler::MapIterator>()
                .map_err(|_| rustler::Error::BadArg)?
            {
                let known = if let Ok(binary) = key.decode::<rustler::Binary>() {
                    name_to_index.get(binary.as_slice()).copied()
                } else {
                    let name = term_to_string(key)?;
                    name_to_index.get(name.as_bytes()).copied()
                };
                let Some(column_index) = known else {
                    return Ok(None);
                };
                row_masks[row_index * words + column_index / 64] |= 1 << (column_index % 64);
            }
        }

        row_terms.push(RowTerms {
            fields,
            tags,
            timestamp,
        });
    }

    let mut union = vec![0u64; words];
    let first = row_masks[..words].to_vec();
    let mut all_same = true;
    for row_index in 0..rows_term.len() {
        let mask = &row_masks[row_index * words..(row_index + 1) * words];
        all_same &= mask == first;
        for (word, value) in mask.iter().enumerate() {
            union[word] |= value;
        }
    }

    let column_indices = columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let timestamp =
                SemanticType::try_from(column.semantic_type) == Ok(SemanticType::Timestamp);
            let present = (union[index / 64] & (1 << (index % 64))) != 0;
            (timestamp || present).then_some(index)
        })
        .collect();

    Ok(Some(KnownBatchPlan {
        row_terms,
        row_masks,
        words,
        all_same,
        column_indices,
    }))
}

/// Encodes rows from a known-column `KnownBatchPlan`.
pub fn terms_to_proto_rows_from_plan<'a>(
    env: rustler::Env<'a>,
    columns: &[ColumnSchema],
    column_indices: &[usize],
    row_terms: &[RowTerms<'a>],
    row_masks: &[u64],
    words: usize,
    defaults: &HashMap<String, ColumnDefault>,
) -> rustler::NifResult<Vec<ProtoRow>> {
    if row_terms.is_empty() {
        return Ok(Vec::new());
    }

    let col_meta: Vec<PlanColumnMeta<'a>> = column_indices
        .iter()
        .map(|&original_index| {
            let column = &columns[original_index];
            (
                original_index,
                SemanticType::try_from(column.semantic_type).unwrap_or(SemanticType::Field),
                column.column_name.encode(env),
                ColumnDataType::try_from(column.datatype).unwrap_or(ColumnDataType::String),
                is_json2_column(column),
                defaults
                    .get(&column.column_name)
                    .map(|default| parse_column_default(default, column)),
            )
        })
        .collect();

    let mut rows = Vec::with_capacity(row_terms.len());
    for (row_index, row) in row_terms.iter().enumerate() {
        let mask = &row_masks[row_index * words..(row_index + 1) * words];
        let mut values = Vec::with_capacity(col_meta.len());
        for (original_index, semantic, key_term, dtype, is_json2, default) in &col_meta {
            let present = *semantic == SemanticType::Timestamp
                || (words > 0 && (mask[original_index / 64] & (1 << (original_index % 64))) != 0);
            let value = if present {
                let val_term = match semantic {
                    SemanticType::Field => row.fields.and_then(|map| map.map_get(*key_term).ok()),
                    SemanticType::Tag => row.tags.and_then(|map| map.map_get(*key_term).ok()),
                    SemanticType::Timestamp => row.timestamp,
                };
                match val_term {
                    Some(term) => encode_term_value(term, *dtype, *is_json2)?,
                    None => none_value(),
                }
            } else if let Some(default) = default {
                match default {
                    Ok(value) => value.clone(),
                    Err(reason) => {
                        return Err(rustler::Error::RaiseTerm(Box::new(reason.clone())));
                    }
                }
            } else {
                none_value()
            };
            values.push(value);
        }
        rows.push(ProtoRow { values });
    }
    Ok(rows)
}

/// Keeps the columns provided by a row group, plus timestamp columns.
pub fn retain_present_columns(
    columns: &[ColumnSchema],
    present: &HashSet<String>,
) -> Vec<ColumnSchema> {
    columns
        .iter()
        .filter(|column| {
            matches!(
                SemanticType::try_from(column.semantic_type),
                Ok(SemanticType::Timestamp)
            ) || present.contains(&column.column_name)
        })
        .cloned()
        .collect()
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

    #[test]
    fn json2_nesting_is_limited_by_the_sdk_parser() {
        // The SDK's json2_value still uses serde_json's default recursion
        // limit. Remove this assertion when the SDK adopts a depth-safe parser;
        // legacy JSON already accepts these documents.
        let depth = 150;
        let doc = format!("{{\"nested\":{}1{}}}", "[".repeat(depth), "]".repeat(depth));
        let err = json2_value(&doc).expect_err("deep JSON2 document should be rejected");
        assert!(
            err.to_string().contains("recursion limit"),
            "expected a recursion-limit error, got: {err}"
        );
    }

    #[test]
    fn preserves_raw_string_defaults_for_mixed_batches() {
        let string_col = field("label", ColumnDataType::String);
        for raw in [" sensor ", "'quoted'", "a(b", "x::y", "NULL", "null", ""] {
            let value = parse_default_value(raw, &string_col).expect("raw string default");
            assert!(matches!(
                value.value_data,
                Some(ValueData::StringValue(ref value)) if value == raw
            ));
        }

        let value = parse_column_default(&ColumnDefault::Null, &string_col).expect("SQL NULL");
        assert!(value.value_data.is_none());
        let value = parse_column_default(&ColumnDefault::Value("NULL".to_string()), &string_col)
            .expect("string NULL");
        assert!(matches!(
            value.value_data,
            Some(ValueData::StringValue(ref value)) if value == "NULL"
        ));
        let value = parse_column_default(&ColumnDefault::Value(String::new()), &string_col)
            .expect("empty string");
        assert!(matches!(
            value.value_data,
            Some(ValueData::StringValue(ref value)) if value.is_empty()
        ));
    }

    #[test]
    fn parses_literal_and_temporal_defaults_for_mixed_batches() {
        let int_col = field("count", ColumnDataType::Int64);
        let value = parse_default_value("42", &int_col).expect("integer default");
        assert!(matches!(value.value_data, Some(ValueData::I64Value(42))));

        let bool_col = field("enabled", ColumnDataType::Boolean);
        let value = parse_default_value("true", &bool_col).expect("boolean default");
        assert!(matches!(value.value_data, Some(ValueData::BoolValue(true))));

        let date_col = field("d", ColumnDataType::Date);
        let value = parse_default_value("2022-06-15", &date_col).expect("date default");
        assert!(matches!(
            value.value_data,
            Some(ValueData::DateValue(19158))
        ));

        let timestamp_col = timestamp("ts", ColumnDataType::TimestampMillisecond);
        let value = parse_default_value("1970-01-01 00:00:00+0000", &timestamp_col)
            .expect("timestamp default");
        assert!(matches!(
            value.value_data,
            Some(ValueData::TimestampMillisecondValue(0))
        ));
        let value = parse_default_value("1969-12-31 23:59:59.997+0000", &timestamp_col)
            .expect("fractional timestamp default");
        assert!(matches!(
            value.value_data,
            Some(ValueData::TimestampMillisecondValue(-3))
        ));
    }

    #[test]
    fn rejects_unparseable_temporal_defaults() {
        let timestamp_col = timestamp("ts", ColumnDataType::TimestampMillisecond);
        assert!(parse_default_value("current_timestamp()", &timestamp_col).is_err());
    }
}
