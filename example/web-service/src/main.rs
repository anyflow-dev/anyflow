#[macro_use]
extern crate lazy_static;
extern crate redis;
use anyflow::dag::OpResults;
use anyflow::AnyHandler;
use anyflow::HandlerInfo;
use anyflow::OpResult;
use async_trait::async_trait;
use macros::AnyFlowNode;
use redis::Commands;
use redis::Connection;
use redis::RedisResult;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;

const REDIS_ADDR: &str = "redis://127.0.0.1/";

struct Req {
    id: i32,
}

#[derive(Deserialize)]
struct Val {
    key: String,
}

#[AnyFlowNode(Val)]
fn redis_op(graph_args: Arc<Req>, p: Val, input: Arc<OpResults>) -> OpResult {
    let mut r = OpResult::default();
    let count: i32 = input
        .get::<Mutex<Connection>>(0)
        .unwrap()
        .lock()
        .unwrap()
        .get(&p.key)
        .unwrap();
    OpResult::ok((p.key.clone(), count))
}

#[AnyFlowNode(Val)]
fn redis_con_op(graph_args: Arc<Req>, p: Val, input: Arc<OpResults>) -> OpResult {
    let mut con: Mutex<Connection> = Mutex::new(
        redis::Client::open(REDIS_ADDR)
            .unwrap()
            .get_connection()
            .unwrap(),
    );
    OpResult::ok(con)
}

fn main() {
    println!("Hello, world!");
}
