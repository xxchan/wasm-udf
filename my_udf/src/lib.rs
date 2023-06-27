use std::sync::Arc;

use arrow_array::Array;

// wit_bindgen::generate!(in "../wit");
// use exports::rw::udf::{udf_v1, udf_v2};

wit_bindgen::generate!({
    path:"../wit"
});

// Define a custom type and implement the generated `udf_v1::Udf` trait for it which
// represents implementing all the necesssary exported interfaces for this
// component.
struct MyUdf;

export_rw!(MyUdf);

// input I64Array, output BoolArray (true if > 0)
impl udf_v1::UdfV1 for MyUdf {
    fn init(_inputs: wit_bindgen::rt::vec::Vec<udf_v1::Scalar>) -> Result<(), udf_v1::InitErrno> {
        todo!()
    }

    fn eval(batch: udf_v1::RecordBatch) -> Result<udf_v1::RecordBatch, udf_v1::EvalErrno> {
        // Read data from IPC buffer
        let batch = arrow_ipc::reader::StreamReader::try_new(batch.as_slice(), None).unwrap();

        // Do UDF computation (for each batch, for each row, do scalar -> scalar)
        let mut ret = arrow_array::builder::BooleanBuilder::new();
        for batch in batch {
            let batch = batch.unwrap();
            for i in 0..batch.num_rows() {
                let val = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(i);
                ret.append_value(val > 0);
            }
        }

        // Write data to IPC buffer
        let mut buf = vec![];
        {
            let array = ret.finish();
            let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "result",
                arrow_schema::DataType::Boolean,
                false,
            )]);
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch =
                arrow_array::RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        Ok(buf)
    }

    fn input_schema() -> udf_v1::Schema {
        todo!()
    }

    fn output_schema() -> udf_v1::Schema {
        todo!()
    }
}

impl udf_v2::UdfV2 for MyUdf {
    fn input_schema() -> udf_v2::Schema {
        todo!()
    }

    fn output_schema() -> udf_v2::Schema {
        todo!()
    }

    fn init(inputs: wit_bindgen::rt::vec::Vec<udf_v2::Scalar>) -> Result<(), udf_v2::InitErrno> {
        todo!()
    }

    fn eval(batch: udf_v2::RecordBatch) -> Result<i32, udf_v2::EvalErrno> {
        // Read data from IPC buffer
        let batch = arrow_ipc::reader::StreamReader::try_new(batch.as_slice(), None).unwrap();

        // Do UDF computation (for each batch, for each row, do scalar -> scalar)
        let mut ret = arrow_array::builder::BooleanBuilder::new();
        for batch in batch {
            let batch = batch.unwrap();
            for i in 0..batch.num_rows() {
                let val = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(i);
                ret.append_value(val > 0);
            }
        }

        // expose data as arrow C interface
        use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};

        let ptr = &FFI_ArrowArray::new(&ret.finish().to_data());
        // return raw pointer
        Ok(ptr as *const FFI_ArrowArray as i32)
    }
}

impl udf_v3::UdfV3 for MyUdf {
    fn input_schema() -> udf_v3::Schema {
        todo!()
    }

    fn output_schema() -> udf_v3::Schema {
        todo!()
    }

    fn init(inputs: wit_bindgen::rt::vec::Vec<udf_v3::Scalar>) -> Result<(), udf_v3::InitErrno> {
        todo!()
    }

    fn eval(batch: udf_v3::RecordBatch) -> Result<udf_v3::ArrayData, udf_v3::EvalErrno> {
        let batch: arrow_array::RecordBatch = batch.into();

        let mut ret = arrow_array::builder::BooleanBuilder::new();

        for i in 0..batch.num_rows() {
            let val = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(i);
            ret.append_value(val > 0);
        }

        let array = ret.finish();
        Ok(array.to_data().into())
    }
}

// WIT array <-> arrow-rs array
mod convert {
    use core::panic;

    use super::*;

    impl From<udf_v3::RecordBatch> for arrow_array::RecordBatch {
        fn from(batch: udf_v3::RecordBatch) -> Self {
            arrow_array::RecordBatch::try_new(
                Arc::new(batch.schema.into()),
                batch
                    .columns
                    .into_iter()
                    .map(|data| {
                        let data: arrow_data::ArrayData = data.into();
                        let arr: Arc<dyn Array> = match data.data_type() {
                            arrow_schema::DataType::Boolean => {
                                Arc::new(arrow_array::BooleanArray::from(data))
                            }
                            arrow_schema::DataType::Int8 => {
                                Arc::new(arrow_array::Int8Array::from(data))
                            }
                            arrow_schema::DataType::Int16 => {
                                Arc::new(arrow_array::Int16Array::from(data))
                            }
                            arrow_schema::DataType::Int32 => {
                                Arc::new(arrow_array::Int32Array::from(data))
                            }
                            arrow_schema::DataType::Int64 => {
                                Arc::new(arrow_array::Int64Array::from(data))
                            }
                            arrow_schema::DataType::Utf8 => {
                                Arc::new(arrow_array::StringArray::from(data))
                            }
                            t => {
                                todo!("unsupported type: {:?}", t);
                            }
                        };
                        arr
                    })
                    .collect(),
            )
            .unwrap()
        }
    }

    impl From<arrow_array::RecordBatch> for udf_v3::RecordBatch {
        fn from(batch: arrow_array::RecordBatch) -> Self {
            udf_v3::RecordBatch {
                schema: batch.schema().as_ref().into(),
                columns: batch
                    .columns()
                    .iter()
                    .map(|arr| arr.into_data().into())
                    .collect(),
            }
        }
    }

    impl From<udf_v3::Schema> for arrow_schema::Schema {
        fn from(schema: udf_v3::Schema) -> Self {
            arrow_schema::Schema::new(
                schema
                    .fields
                    .into_iter()
                    .map(|f| f.into())
                    .collect::<Vec<arrow_schema::Field>>(),
            )
        }
    }

    impl From<&arrow_schema::Schema> for udf_v3::Schema {
        fn from(schema: &arrow_schema::Schema) -> Self {
            udf_v3::Schema {
                fields: schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().into())
                    .collect::<Vec<udf_v3::Field>>(),
            }
        }
    }

    impl From<udf_v3::Field> for arrow_schema::Field {
        fn from(field: udf_v3::Field) -> Self {
            arrow_schema::Field::new(field.name, field.data_type.into(), true)
        }
    }

    impl From<&arrow_schema::Field> for udf_v3::Field {
        fn from(field: &arrow_schema::Field) -> Self {
            udf_v3::Field {
                name: field.name().to_string(),
                data_type: field.data_type().into(),
            }
        }
    }

    impl From<udf_v3::DataType> for arrow_schema::DataType {
        fn from(ty: udf_v3::DataType) -> Self {
            match ty {
                udf_v3::DataType::DtI16 => arrow_schema::DataType::Int16,
                udf_v3::DataType::DtI32 => arrow_schema::DataType::Int32,
                udf_v3::DataType::DtI64 => arrow_schema::DataType::Int64,
                udf_v3::DataType::DtBool => arrow_schema::DataType::Boolean,
                udf_v3::DataType::DtString => arrow_schema::DataType::Utf8,
            }
        }
    }

    impl From<&arrow_schema::DataType> for udf_v3::DataType {
        fn from(ty: &arrow_schema::DataType) -> Self {
            match ty {
                arrow_schema::DataType::Int16 => udf_v3::DataType::DtI16,
                arrow_schema::DataType::Int32 => udf_v3::DataType::DtI32,
                arrow_schema::DataType::Int64 => udf_v3::DataType::DtI64,
                arrow_schema::DataType::Boolean => udf_v3::DataType::DtBool,
                arrow_schema::DataType::Utf8 => udf_v3::DataType::DtString,
                _ => todo!(),
            }
        }
    }

    fn parse_buffer(bytes: &[u8]) -> arrow_buffer::Buffer {
        arrow_buffer::Buffer::from(bytes.to_vec())
    }

    impl From<udf_v3::ArrayData> for arrow_data::ArrayData {
        fn from(data: udf_v3::ArrayData) -> Self {
            arrow_data::ArrayData::try_new(
                data.data_type.into(),
                data.len as usize,
                data.nulls.map(|bytes| parse_buffer(&bytes)),
                data.offset as usize,
                data.buffers
                    .into_iter()
                    .map(|bytes| parse_buffer(&bytes))
                    .collect::<Vec<_>>(),
                vec![],
            )
            .unwrap()
        }
    }

    impl From<arrow_data::ArrayData> for udf_v3::ArrayData {
        fn from(value: arrow_data::ArrayData) -> Self {
            udf_v3::ArrayData {
                data_type: value.data_type().into(),
                len: value.len() as u32,
                offset: value.offset() as u32,
                buffers: value
                    .buffers()
                    .iter()
                    .map(|buf| buf.as_slice().to_vec())
                    .collect::<Vec<_>>(),
                nulls: value.nulls().map(|buf| buf.buffer().as_slice().to_vec()),
            }
        }
    }
}
