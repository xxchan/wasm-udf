use std::sync::Arc;

use wasmtime::component::*;
use wasmtime::{Config, Engine, Store};

bindgen!({
    // TODO: I don't understant very much why async is required (for wasi).
    // thread 'main' panicked at 'cannot use `func_wrap_async` without enabling async support in the config', /Users/xxchan/.cargo/git/checkouts/wasmtime-41807828cb3a7a7e/5b93cdb/crates/wasmtime/src/component/linker.rs:287:9
    async: true,
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
async fn main() -> wasmtime::Result<()> {
    // Configure an `Engine` and compile the `Component` that is being run for
    // the application.
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.async_support(true);

    let engine = Engine::new(&config)?;
    let t1 = std::time::Instant::now();
    let component = Component::from_file(&engine, "my_udf.wasm")?;
    println!(
        "load wasm_component.wasm: {:?}",
        std::time::Instant::now() - t1
    );
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
    let mut store = Store::new(&engine, MyState { table, wasi_ctx });
    let (bindings, _) = Rw::instantiate_async(&mut store, &component, &linker).await?;

    // wasmtime_wasi::preview2::wasi::command::add_to_linker(&mut linker)?;
    // let (_bindings_wasi, _) = Rw::instantiate_async(&mut store, &component_wasi, &linker).await?;

    let input = arrow_array::Int64Array::from(vec![-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]);

    // put input into IPC buffer
    let mut buf = vec![];
    {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "input",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
        let batch =
            arrow_array::RecordBatch::try_new(Arc::new(schema), vec![Arc::new(input)]).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    // call UDF.
    let output = bindings.udf_v1().call_eval(&mut store, &buf).await??;

    // Get output from IPC buffer
    let batch = arrow_ipc::reader::StreamReader::try_new(output.as_slice(), None).unwrap();
    for batch in batch {
        let batch = batch.unwrap();
        println!("{:?}", batch);
    }

    Ok(())
}
