
use std::cell::RefCell;

use anyhow::Result;
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
// use sysinfo::System;
// use sysinfo::SystemExt;

#[global_allocator]
pub static GLOBAL: &StatsAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;


pub fn instrument<T>(title: &str, f: impl FnOnce() -> T) -> T {
    println!("{:=^50}", "");
    println!("|{:^48}|", title);
    println!("{:=^50}", "");

    let region = Region::new(&GLOBAL);
    let t = std::time::Instant::now();
    // -------------
    let ret = f();
    // -------------
    let elapsed = t.elapsed();

    println!(
        "elapsed: {elapsed:?}\nused memory: {}
",
        humansize::format_size(
            region.change().bytes_allocated - region.change().bytes_deallocated,
            humansize::BINARY
        ),
    );
    ret
}
