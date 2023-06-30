#![allow(unused)]

use std::sync::Arc;

use arrow_array::Array;
use wasmtime::{component::*, AsContextMut};
use wasmtime::{Config, Engine, Store};

mod utils;
use utils::instrument;

bindgen!({
    // TODO: I don't understant very much why async is required (for wasi).
    // thread 'main' panicked at 'cannot use `func_wrap_async` without enabling async support in the config', /Users/xxchan/.cargo/git/checkouts/wasmtime-41807828cb3a7a7e/5b93cdb/crates/wasmtime/src/component/linker.rs:287:9
    // async: true,
});

struct MyState {
    wasi_ctx: wasmtime_wasi::preview2::WasiCtx,
    table: wasmtime_wasi::preview2::Table,
}

impl wasmtime_wasi::preview2::WasiView for MyState {
    fn table(&self) -> &wasmtime_wasi::preview2::Table {
        &self.table
    }

    fn table_mut(&mut self) -> &mut wasmtime_wasi::preview2::Table {
        &mut self.table
    }

    fn ctx(&self) -> &wasmtime_wasi::preview2::WasiCtx {
        &self.wasi_ctx
    }

    fn ctx_mut(&mut self) -> &mut wasmtime_wasi::preview2::WasiCtx {
        &mut self.wasi_ctx
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Configure an `Engine` and compile the `Component` that is being run for
    // the application.
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.async_support(false);

    let engine = instrument("create engine", || Engine::new(&config))?;
    let component = instrument("load component", || {
        Component::from_file(&engine, "my_udf.wasm")
    })?;
    // let component_wasi = Component::from_file(&engine, "wasm_component_wasi.wasm")?;

    let mut linker = Linker::new(&engine);

    // As with the core wasm API of Wasmtime instantiation occurs within a
    // `Store`. The bindings structure contains an `instantiate` method which
    // takes the store, component, and linker. This returns the `bindings`
    // structure which is an instance of `HelloWorld` and supports typed access
    // to the exports of the component.
    let mut table = wasmtime_wasi::preview2::Table::new();
    let wasi_ctx = wasmtime_wasi::preview2::WasiCtxBuilder::new()
        .inherit_stdio() // this is needed for println to work
        .build(&mut table)?;
    let mut store = instrument("create store", || {
        Store::new(&engine, MyState { table, wasi_ctx })
    });
    let (bindings, instance) = Rw::instantiate(&mut store, &component, &linker)?;

    // WASI
    // config.async_support(true);
    // let (bindings, instance) = Rw::instantiate_async(&mut store, &component, &linker).aait?;

    // wasmtime_wasi::preview2::wasi::command::add_to_linker(&mut linker)?;
    // let (_bindings_wasi, _) = Rw::instantiate_async(&mut store, &component_wasi, &linker).await?;

    let input = arrow_array::Int64Array::from(vec![-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]);
    let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "input",
        arrow_schema::DataType::Int64,
        false,
    )]);
    let schema = Arc::new(schema);

    // V1: Use Arrow IPC to pass data. TODO: can we write to WASM memory directly?
    {
        let input = input.clone();
        let schema = schema.clone();
        // put input into IPC buffer
        let mut buf = vec![];
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch = arrow_array::RecordBatch::try_new(schema, vec![Arc::new(input)]).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // call UDF.
        let output = bindings.udf_v1().call_eval(&mut store, &buf)??;

        // Get output from IPC buffer
        let batch = arrow_ipc::reader::StreamReader::try_new(output.as_slice(), None).unwrap();
        for batch in batch {
            let batch = batch.unwrap();
            println!("{:?}", batch);
        }
    }

    // V2: Use Arrow C data interface (DOES NOT WORK YET)
    {
        use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};

        let input = input.clone();
        let schema = schema.clone();

        // put input into IPC buffer
        // host memory cannot be accessed in guest, so we still need to copy data into guest memory first.
        let mut buf = vec![];
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch = arrow_array::RecordBatch::try_new(schema, vec![Arc::new(input)]).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // call UDF.
        let output = bindings.udf_v2().call_eval(&mut store, &buf)??;
        let ffi = output;
        // TODO: How to access wasm memory of the component???
    }

    // V3: Use wit to describe an Arrow Chunk
    {
        let input = input.clone();
        let schema = schema.clone();

        let batch =
            arrow_array::RecordBatch::try_new(schema.clone(), vec![Arc::new(input)]).unwrap();

        // TODO: why does RecordBatch has a lifetime? It's different from wit-bindgen. And we cannot use .into() easily.
        // https://github.com/bytecodealliance/wasmtime/issues/6645
        let schema = schema.as_ref().into();
        let columns: Vec<udf_v3::ArrayData> = batch
            .columns()
            .iter()
            .map(|array| array.into_data().into())
            .collect::<Vec<_>>();
        let columns = columns.iter().collect::<Vec<_>>();

        let batch = udf_v3::RecordBatch {
            schema: &schema,
            columns: &columns,
        };

        let output = bindings.udf_v3().call_eval(&mut store, batch)??;
        let data = arrow_data::ArrayData::from(output);
        let array = arrow_array::BooleanArray::from(data);
        println!("{:?}", array);
    }

    Ok(())
}

// fn parse_wasm_ffi_array(ptr: i32) ->  {

// }

// WIT array <-> arrow-rs array
// TODO: this is copied from guest code. (And modified, since there are some differences)
// Can we share this code between host and guest(s)?
mod convert {
    use super::*;

    impl From<udf_v3::RecordBatch<'_>> for arrow_array::RecordBatch {
        fn from(batch: udf_v3::RecordBatch) -> Self {
            arrow_array::RecordBatch::try_new(
                Arc::new(batch.schema.clone().into()),
                batch
                    .columns
                    .into_iter()
                    .map(|&data| {
                        let data: arrow_data::ArrayData = data.clone().into();
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
                            arrow_schema::DataType::Utf8 => {
                                Arc::new(arrow_array::StringArray::from(data))
                            }
                            _ => todo!(),
                        };
                        arr
                    })
                    .collect(),
            )
            .unwrap()
        }
    }

    // impl From<&arrow_array::RecordBatch> for udf_v3::RecordBatch<'_> {
    //     fn from(batch: &arrow_array::RecordBatch) -> Self {
    //         udf_v3::RecordBatch {
    //             schema: batch.schema().as_ref().into(),
    //             columns: batch
    //                 .columns()
    //                 .iter()
    //                 .map(|arr| arr.into_data().into())
    //                 .collect(),
    //         }
    //     }
    // }

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
