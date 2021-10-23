

use serde_json::value::RawValue;
use std::fs;
use std::sync::Arc;
use serde_json::{Result, Value};
use serde::{Deserialize, Serialize};
use pprof;
use std::fs::File;

#[derive(Serialize, Deserialize, Debug)]
struct Val {
    val: i32
}

fn calc<'a, E: Send + Sync>(
    _graph_args: &'a Arc<E>,
    input: Arc<anyflow::dag::NodeResult>,
    params: Box<RawValue>,
) -> anyflow::dag::NodeResult {
    let p: Val = serde_json::from_str(params.get()).unwrap();
    

    let val = match input.get::<i32>("res") {
        Some(val) => val,
        None => 0,
    };
    // println!("params {:?} {:?}", p, val);
    anyflow::dag::NodeResult::new().set("res", &(val + p.val))
}

#[tokio::main]
async fn main() {
    // let guard = pprof::ProfilerGuard::new(100).unwrap();

    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    dag.register("calc", Arc::new(calc));
    for i in 0..100000000 {
        let my_dag = dag.make_flow(Arc::new(1));
        // println!("{:?}", my_dag.await[0].get::<i32>("res"));
    }

    // if let Ok(report) = guard.report().build() {
    //     let file = File::create("flamegraph.svg").unwrap();
    //     let mut options = pprof::flamegraph::Options::default();
    //     options.image_width = Some(1800);
    //     report.flamegraph_with_options(file, &mut options).unwrap();
    // };
}
