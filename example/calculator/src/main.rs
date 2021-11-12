use async_std::task;
use futures::future::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fs;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
// use anyflow::FlowResult;
use anyflow::dag::NodeResults;
use macros::AnyFlowNode;
use anyflow::resgiter_node;
use anyflow::NodeResult;
use anyflow;
use anyflow::HandlerInfo;
use anyflow::HandlerType;
use anyflow::AnyHandler;
use async_trait::async_trait;

#[derive(Serialize, Deserialize, Debug, Default)]
struct Val {
    val: i32,
}

#[AnyFlowNode(Val)]
fn calc<E: Send + Sync>(
    _graph_args: Arc<E>,
    params: Box<RawValue>,
    input: Arc<NodeResults>,
) -> NodeResult {
    let p: Val = serde_json::from_str(params.get()).unwrap();

    let mut r = NodeResult::default();
    let mut sum: i32 = 0;

    for idx in 0..input.len() {
        match input.get::<i32>(idx) {
            Ok(val) => sum += val,
            Err(e) => {}
        }
    }

    NodeResult::ok(sum + p.val)
}


fn smol_main() {
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    // dag.register("calc", Arc::new(calc));
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
    // dag.register("calc", Arc::new(calc));
    // dag.async_register("calc", async_calc);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    for _i in 0..10000 {
        let my_dag = dag.make_flow(Arc::new(1));
        // println!("{:?}", my_dag.await[0].get::<i32>("res"));
        rt.block_on(my_dag);
    }

    match guard.report().build() {
        Ok(report) => {
            let file = File::create("flamegraph.svg").unwrap();
            report.flamegraph(file).unwrap();

            // println!("{:?}", report);
        }
        Err(_) => {}
    };
}

fn async_std_main() {
    // let guard = pprof::ProfilerGuard::new(100).unwrap();

    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    // dag.register("calc", Arc::new(calc));
    // dag.async_register("calc", async_calc);
    dag.multi_async_register(resgiter_node![calc]);
    for _i in 0..10 {
        let my_dag = dag.make_flow(Arc::new(1));
        let r = task::block_on(my_dag);
        println!("result {:?}", r[0].get::<i32>());
    }

    // match guard.report().build() {
    //     Ok(report) => {
    //         let file = File::create("flamegraph.svg").unwrap();
    //         report.flamegraph(file).unwrap();

    //         // println!("{:?}", report);
    //     }
    //     Err(_) => {}
    // };
}

fn main() {
    async_std_main();
    // tokio_main();
    let q: i32 = 5;
    let mut a = anyflow::NodeResult::Ok(Arc::new(Mutex::new(5)));
    let p = a.get::<Mutex<i32>>();
    // let mut u = ;
    *p.unwrap().lock().unwrap() = 6;
    println!("{:?}", a.get::<Mutex<i32>>());
    // let aa = resgiter_node![async_calc_handler_fn];
}
