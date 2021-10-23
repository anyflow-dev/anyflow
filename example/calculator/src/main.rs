use pprof;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Result, Value};
use smol::{io, net, prelude::*, Unblock};
use std::fs;
use std::fs::File;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
struct Val {
    val: i32,
}

fn calc<'a, E: Send + Sync>(
    _graph_args: &'a Arc<E>,
    input: Arc<anyflow::dag::FlowResult>,
    params: Box<RawValue>,
) -> anyflow::dag::FlowResult {
    let p: Val = serde_json::from_str(params.get()).unwrap();

    let val: i32 = match input.get::<i32>("res") {
        Ok(val) => val.clone(),
        Err(e) => 0,
    };
    // println!("params {:?} {:?}", p, val);
    let mut r = anyflow::dag::FlowResult::new();
    r.set("res", val + p.val);
    r
}

fn smol_main() {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    dag.register("calc", Arc::new(calc));
    for i in 0..100000 {
        let my_dag = dag.make_flow(Arc::new(1));
        // println!("{:?}", my_dag.await[0].get::<i32>("res"));
        smol::block_on(my_dag);
    }
}

fn tokio_main() {
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    dag.register("calc", Arc::new(calc));
    let rt = tokio::runtime::Runtime::new().unwrap();
    for i in 0..100000 {
        let my_dag = dag.make_flow(Arc::new(1));
        // println!("{:?}", my_dag.await[0].get::<i32>("res"));
        rt.block_on(my_dag);
    }

    if let Ok(report) = guard.report().build() {
        let file = File::create("flamegraph.svg").unwrap();
        let mut options = pprof::flamegraph::Options::default();
        options.image_width = Some(1800);
        report.flamegraph_with_options(file, &mut options).unwrap();
    };
}

fn main() {
    tokio_main();
}
