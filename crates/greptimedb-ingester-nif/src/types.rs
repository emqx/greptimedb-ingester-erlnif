use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use greptimedb_ingester::api::v1::value::ValueData;
use greptimedb_ingester::api::v1::{ColumnDataType, Value as ProtoValue};
use greptimedb_ingester::Value;
use rustler::{Encoder, Env, Term};

macro_rules! match_term_to_value {
    ($val:expr, $dtype:expr, [ $( ($variant:ident, $rust_type:ty) ),* ]) => {
        match $dtype {
            $(
                ColumnDataType::$variant => {
                    if let Ok(v) = $val.decode::<$rust_type>() {
                        Ok(Value::$variant(v))
                    } else {
                        Ok(Value::Null)
                    }
                }
            )*
            _ => Ok(Value::Null),
        }
    };
}

pub fn term_to_value(val: &Term, dtype: ColumnDataType) -> rustler::NifResult<Value> {
    match_term_to_value!(val, dtype, [
        (Boolean, bool),
        (Int8, i8),
        (Int16, i16),
        (Int32, i32),
        (Int64, i64),
        (Uint8, u8),
        (Uint16, u16),
        (Uint32, u32),
        (Uint64, u64),
        (Float32, f32),
        (Float64, f64),
        (String, String),
        (Binary, Vec<u8>),
        (Date, i32),
        (Datetime, i64),
        (TimestampSecond, i64),
        (TimestampMillisecond, i64),
        (TimestampMicrosecond, i64),
        (TimestampNanosecond, i64)
    ])
}

pub fn value_to_proto_value(value: Value) -> ProtoValue {
    let value_data = match value {
        Value::Boolean(v) => Some(ValueData::BoolValue(v)),
        Value::Int8(v) => Some(ValueData::I8Value(v as i32)),
        Value::Int16(v) => Some(ValueData::I16Value(v as i32)),
        Value::Int32(v) => Some(ValueData::I32Value(v)),
        Value::Int64(v) => Some(ValueData::I64Value(v)),
        Value::Uint8(v) => Some(ValueData::U8Value(v as u32)),
        Value::Uint16(v) => Some(ValueData::U16Value(v as u32)),
        Value::Uint32(v) => Some(ValueData::U32Value(v)),
        Value::Uint64(v) => Some(ValueData::U64Value(v)),
        Value::Float32(v) => Some(ValueData::F32Value(v)),
        Value::Float64(v) => Some(ValueData::F64Value(v)),
        Value::String(v) => Some(ValueData::StringValue(v)),
        Value::Binary(v) => Some(ValueData::BinaryValue(v)),
        Value::Date(v) => Some(ValueData::DateValue(v)),
        Value::Datetime(v) => Some(ValueData::DatetimeValue(v)),
        Value::TimestampSecond(v) => Some(ValueData::TimestampSecondValue(v)),
        Value::TimestampMillisecond(v) => Some(ValueData::TimestampMillisecondValue(v)),
        Value::TimestampMicrosecond(v) => Some(ValueData::TimestampMicrosecondValue(v)),
        Value::TimestampNanosecond(v) => Some(ValueData::TimestampNanosecondValue(v)),
        Value::Null => None,
        Value::Json(v) => Some(ValueData::StringValue(v)),
        Value::TimeSecond(v) => Some(ValueData::TimeSecondValue(v as i64)),
        Value::TimeMillisecond(v) => Some(ValueData::TimeMillisecondValue(v as i64)),
        Value::TimeMicrosecond(v) => Some(ValueData::TimeMicrosecondValue(v)),
        Value::TimeNanosecond(v) => Some(ValueData::TimeNanosecondValue(v)),
        Value::Decimal128(_v) => None,
    };
    ProtoValue { value_data }
}

pub fn term_to_proto_value(val: &Term, dtype: ColumnDataType) -> rustler::NifResult<ProtoValue> {
    let value = term_to_value(val, dtype)?;
    Ok(value_to_proto_value(value))
}

pub fn record_batch_to_terms<'a>(env: Env<'a>, batch: &RecordBatch) -> Vec<Term<'a>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        let mut row_values = Vec::with_capacity(num_cols);
        for j in 0..num_cols {
            let col = batch.column(j);
            row_values.push(array_value_to_term(env, col, i));
        }
        rows.push(row_values.encode(env));
    }
    rows
}

macro_rules! match_array_value {
    ($array:expr, $index:expr, $env:expr, [ $( ($dtype_pat:pat, $array_type:ty) ),* ]) => {
        match $array.data_type() {
            $(
                $dtype_pat => $array
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .expect("valid cast")
                    .value($index)
                    .encode($env),
            )*
            _ => format!("Unsupported(Type: {:?})", $array.data_type()).encode($env),
        }
    };
}

fn array_value_to_term<'a>(env: Env<'a>, array: &ArrayRef, index: usize) -> Term<'a> {
    if array.is_null(index) {
        return rustler::types::atom::nil().to_term(env);
    }
    match_array_value!(array, index, env, [
        (DataType::Boolean, BooleanArray),
        (DataType::Int8, Int8Array),
        (DataType::Int16, Int16Array),
        (DataType::Int32, Int32Array),
        (DataType::Int64, Int64Array),
        (DataType::UInt8, UInt8Array),
        (DataType::UInt16, UInt16Array),
        (DataType::UInt32, UInt32Array),
        (DataType::UInt64, UInt64Array),
        (DataType::Float32, Float32Array),
        (DataType::Float64, Float64Array),
        (DataType::Utf8, StringArray),
        (DataType::LargeUtf8, LargeStringArray),
        (DataType::Binary, BinaryArray),
        (DataType::LargeBinary, LargeBinaryArray),
        (DataType::Date32, Date32Array),
        (DataType::Date64, Date64Array),
        (DataType::Timestamp(TimeUnit::Second, _), TimestampSecondArray),
        (DataType::Timestamp(TimeUnit::Millisecond, _), TimestampMillisecondArray),
        (DataType::Timestamp(TimeUnit::Microsecond, _), TimestampMicrosecondArray),
        (DataType::Timestamp(TimeUnit::Nanosecond, _), TimestampNanosecondArray)
    ])
}