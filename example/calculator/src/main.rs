

use serde_json::value::RawValue;
use std::fs;
use std::sync::Arc;


fn calc<'a, E: Send + Sync>(
    _graph_args: &'a Arc<E>,
    _input: Arc<anyflow::dag::NodeResult>,
    _params: Box<RawValue>,
) -> anyflow::dag::NodeResult {
    println!("data");
    let t = anyflow::dag::NodeResult::new();
    t.set("xxx", &anyflow::dag::DAGConfig::default());
    let _c = t.get::<anyflow::dag::DAGConfig>("xxx");
    t
}

#[tokio::main]
async fn main() {
    let mut dag = anyflow::dag::Flow::<i32, i32>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    println!("{:?}", dag.init(&data));
    dag.register("calc", Arc::new(calc));
    let my_dag = dag.make_flow(Arc::new(1));
    println!("{:?}", my_dag.await);
    // let mut rt = tokio::runtime::Runtime::new().unwrap();
    // let result = rt.block_on(my_dag);
}
