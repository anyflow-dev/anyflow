use async_std::task;
use futures::future::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fs;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use anyflow::FlowResult;

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
    // println!("params {:?} {:?}", p, val);
    let mut r = anyflow::FlowResult::new();
    r.set("res", val + p.val);
    r
}

async fn async_calc<E: Send + Sync>(
    _graph_args: Arc<E>,
    input: Arc<anyflow::FlowResult>,
    params: Box<RawValue>,
) -> anyflow::FlowResult {
    let p: Val = serde_json::from_str(params.get()).unwrap();

    let val: i32 = match input.get::<i32>("res") {
        Ok(val) => *val,
        Err(_e) => 0,
    };
    println!("params {:?} {:?}", p, val);
    let mut r = anyflow::FlowResult::new();
    r.set("res", val + p.val);
    r
}

fn smol_main() {
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    dag.register("calc", Arc::new(calc));
    for _i in 0..1000 {
        let my_dag = dag.make_flow(Arc::new(1));
        // println!("{:?}", my_dag.await[0].get::<i32>("res"));
        smol::block_on(my_dag);
    }
    match guard.report().build() {
        Ok(report) => {
            let file = File::create("flamegraph.svg").unwrap();
            report.flamegraph(file).unwrap();

            println!("{:?}", report);
        }
        Err(_) => {}
    };
}

fn tokio_main() {
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    dag.register("calc", Arc::new(calc));
    let rt = tokio::runtime::Runtime::new().unwrap();
    for _i in 0..1000 {
        let my_dag = dag.make_flow(Arc::new(1));
        // println!("{:?}", my_dag.await[0].get::<i32>("res"));
        rt.block_on(my_dag);
    }

    match guard.report().build() {
        Ok(report) => {
            let file = File::create("flamegraph.svg").unwrap();
            report.flamegraph(file).unwrap();

            println!("{:?}", report);
        }
        Err(_) => {}
    };
}

fn async_std_main() {
    // let guard = pprof::ProfilerGuard::new(100).unwrap();

    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    dag.register("calc", Arc::new(calc));
    dag.async_register("calc", async_calc);
    // dag.async_register("async_calc", Arc::new(async_calc));
    for _i in 0..1 {
        let my_dag = dag.make_flow(Arc::new(1));
        task::block_on(my_dag);
    }

    // match guard.report().build() {
    //     Ok(report) => {
    //         let file = File::create("flamegraph.svg").unwrap();
    //         report.flamegraph(file).unwrap();

    //         println!("{:?}", report);
    //     }
    //     Err(_) => {}
    // };
}

fn main() {
    // async_std_main();
    let mut a = anyflow::FlowResult::new();
    let q: i32 = 5;
    a.set("ppp", Mutex::new(q));
    let p = a.get::<Mutex<i32>>("ppp");
    // let mut u = ;
    *p.unwrap().lock().unwrap() = 6;
    println!("{:?}", a.get::<Mutex<i32>>("ppp"));
}
