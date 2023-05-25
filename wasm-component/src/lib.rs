use std::sync::Arc;

wit_bindgen::generate!({
    path:"../wit"
});

// Define a custom type and implement the generated `udf::Udf` trait for it which
// represents implementing all the necesssary exported interfaces for this
// component.
struct MyUdf;

// input I64Array, output BoolArray (true if > 0)
impl udf::Udf for MyUdf {
    fn init(_inputs: wit_bindgen::rt::vec::Vec<udf::Scalar>) -> Result<(), udf::InitErrno> {
        todo!()
    }

    fn eval(batch: udf::RecordBatch) -> Result<udf::RecordBatch, udf::EvalErrno> {
        let batch = arrow_ipc::reader::StreamReader::try_new(batch.as_slice(), None).unwrap();

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

        let array = ret.finish();
        let mut buf = vec![];
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "result",
            arrow_schema::DataType::Boolean,
            false,
        )]);
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch =
                arrow_array::RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        Ok(buf)
    }

    fn input_schema() -> udf::Schema {
        todo!()
    }

    fn output_schema() -> udf::Schema {
        todo!()
    }
}

export_rw!(MyUdf);
