use async_std::task;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use std::fs;

use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
struct Val {
    val: i32,
}

fn calc<'a, E: Send + Sync>(
    _graph_args: &'a Arc<E>,
    input: Arc<anyflow::FlowResult>,
    params: &'a Box<RawValue>,
) -> anyflow::FlowResult {
    let p: Val = serde_json::from_str(params.get()).unwrap();
    let val: i32 = match input.get::<i32>("res") {
        Ok(val) => *val,
        Err(_e) => 0,
    };
    let mut r = anyflow::FlowResult::new();
    r.set("res", val + p.val);
    r
}
fn smol_main(times: i32) {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    dag.init(&data).unwrap();
    dag.register("calc", Arc::new(calc));
    for _i in 0..times {
        let my_dag = dag.make_flow(Arc::new(1));
        smol::block_on(my_dag);
    }
}

fn tokio_main(times: i32) {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    dag.init(&data).unwrap();
    dag.register("calc", Arc::new(calc));
    let rt = tokio::runtime::Runtime::new().unwrap();
    for _i in 0..times {
        let my_dag = dag.make_flow(Arc::new(1));
        rt.block_on(my_dag);
    }
}

fn async_std_main(times: i32) {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    dag.init(&data).unwrap();
    dag.register("calc", Arc::new(calc));
    for _i in 0..times {
        let my_dag = dag.make_flow(Arc::new(1));
        task::block_on(my_dag);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let times: i32 = 1000;
    c.bench_function("tokio", |b| b.iter(|| tokio_main(black_box(times))));
    c.bench_function("smol", |b| b.iter(|| smol_main(black_box(times))));
    c.bench_function("async-std", |b| b.iter(|| async_std_main(black_box(times))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
