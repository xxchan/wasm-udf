use std::sync::Arc;

wit_bindgen::generate!({
    path:"../wit"
});

// Define a custom type and implement the generated `udf_v1::Udf` trait for it which
// represents implementing all the necesssary exported interfaces for this
// component.
struct MyUdf;

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

    fn eval(batch: udf_v2::RecordBatch) -> Result<udf_v2::RecordBatch, udf_v2::EvalErrno> {
        todo!()
    }
}

export_rw!(MyUdf);
