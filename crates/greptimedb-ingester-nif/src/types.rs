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

macro_rules! convert_int {
    ($val:expr, $variant:ident, $intermediate:ty, $target:ty) => {
        match $val.decode::<Option<$intermediate>>() {
            Ok(Some(v)) => match <$target>::try_from(v) {
                Ok(casted) => Ok(Value::$variant(casted)),
                Err(_) => Err(rustler::Error::RaiseTerm(Box::new(format!(
                    "Value {} out of range for type {}",
                    v,
                    stringify!($variant)
                )))),
            },
            Ok(None) => Ok(Value::Null),
            Err(_) => Err(rustler::Error::RaiseTerm(Box::new(format!(
                "Invalid value for type {}: expected integer",
                stringify!($variant)
            )))),
        }
    };
}

macro_rules! convert_direct {
    ($val:expr, $variant:ident, $type:ty) => {
        match $val.decode::<Option<$type>>() {
            Ok(Some(v)) => Ok(Value::$variant(v)),
            Ok(None) => Ok(Value::Null),
            Err(_) => Err(rustler::Error::RaiseTerm(Box::new(format!(
                "Invalid value for type {}",
                stringify!($variant)
            )))),
        }
    };
}

pub fn term_to_value(val: &Term, dtype: ColumnDataType) -> rustler::NifResult<Value> {
    match dtype {
        ColumnDataType::Boolean => convert_direct!(val, Boolean, bool),
        ColumnDataType::Int8 => convert_int!(val, Int8, i64, i8),
        ColumnDataType::Int16 => convert_int!(val, Int16, i64, i16),
        ColumnDataType::Int32 => convert_int!(val, Int32, i64, i32),
        ColumnDataType::Int64 => convert_int!(val, Int64, i64, i64),
        ColumnDataType::Uint8 => convert_int!(val, Uint8, u64, u8),
        ColumnDataType::Uint16 => convert_int!(val, Uint16, u64, u16),
        ColumnDataType::Uint32 => convert_int!(val, Uint32, u64, u32),
        ColumnDataType::Uint64 => convert_int!(val, Uint64, u64, u64),
        ColumnDataType::Float32 => convert_direct!(val, Float32, f32),
        ColumnDataType::Float64 => convert_direct!(val, Float64, f64),
        ColumnDataType::String => convert_direct!(val, String, String),
        ColumnDataType::Binary => {
            if let Ok(bin) = val.decode::<rustler::Binary>() {
                Ok(Value::Binary(bin.as_slice().to_vec()))
            } else {
                convert_direct!(val, Binary, Vec<u8>)
            }
        }
        ColumnDataType::Date => convert_int!(val, Date, i32, i32),
        ColumnDataType::Datetime => convert_int!(val, Datetime, i64, i64),
        ColumnDataType::TimestampSecond => convert_int!(val, TimestampSecond, i64, i64),
        ColumnDataType::TimestampMillisecond => convert_int!(val, TimestampMillisecond, i64, i64),
        ColumnDataType::TimestampMicrosecond => convert_int!(val, TimestampMicrosecond, i64, i64),
        ColumnDataType::TimestampNanosecond => convert_int!(val, TimestampNanosecond, i64, i64),
        _ => Ok(Value::Null),
    }
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
    match_array_value!(
        array,
        index,
        env,
        [
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
            (
                DataType::Timestamp(TimeUnit::Second, _),
                TimestampSecondArray
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, _),
                TimestampMillisecondArray
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                TimestampMicrosecondArray
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, _),
                TimestampNanosecondArray
            )
        ]
    )
}
