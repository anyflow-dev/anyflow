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
    params: &'a Box<RawValue>,
    input: &anyflow::NodeResults,
) -> anyflow::NodeResult {
    let p: Val = serde_json::from_str(params.get()).unwrap();

    let mut r = anyflow::NodeResult::default();
    let mut sum: i32 = 0;
    // println!("xxx{:?}", input.len());

    for idx in 0..input.len() {
        match input.get::<i32>(idx) {
            Ok(val) => sum += val,
            Err(e) => {}
        }
    }

    anyflow::NodeResult::ok(sum + p.val)
}

async fn async_calc<E: Send + Sync>(
    _graph_args: Arc<E>,
    params: Box<RawValue>,
    input: Arc<anyflow::NodeResults>,
) -> anyflow::NodeResult {
    let p: Val = serde_json::from_str(params.get()).unwrap();

    let mut r = anyflow::NodeResult::default();
    let mut sum: i32 = 0;
    // println!("xxx{:?}", input.len());

    for idx in 0..input.len() {
        match input.get::<i32>(idx) {
            Ok(val) => sum += val,
            Err(e) => {}
        }
    }

    anyflow::NodeResult::ok(sum + p.val)
}

fn smol_main(times: i32) {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    dag.init(&data).unwrap();
    dag.register("calc", Arc::new(calc));
    dag.async_register("calc", async_calc);
    for _i in 0..times {
        let my_dag = dag.make_flow(Arc::new(1));
        let r = smol::block_on(my_dag);
        // println!("result {:?}", r[0].get::<i32>());
    }
}

fn tokio_main(times: i32) {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    dag.init(&data).unwrap();
    dag.register("calc", Arc::new(calc));
    dag.async_register("calc", async_calc);
    let rt = tokio::runtime::Runtime::new().unwrap();
    for _i in 0..times {
        let my_dag = dag.make_flow(Arc::new(1));
        let r = rt.block_on(my_dag);
        // println!("xxx {:?}", r);
        // println!("result {:?}", r[0].get::<i32>());
    }
}

fn async_std_main(times: i32) {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    dag.init(&data).unwrap();
    dag.register("calc", Arc::new(calc));
    dag.async_register("calc", async_calc);
    // dag.async_register("async_calc", Arc::new(async_calc));
    for _i in 0..times {
        let my_dag = dag.make_flow(Arc::new(1));
        let r = task::block_on(my_dag);
        // println!("result {:?}", r[0].get::<i32>());
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
