#[macro_use]
extern crate lazy_static;
extern crate redis;
use anyflow::dag::OpResults;
use anyflow::resgiter_node;
use anyflow::AnyHandler;
use anyflow::EmptyPlaceHolder;
use anyflow::HandlerInfo;
use anyflow::OpResult;
use async_trait::async_trait;
use futures::future::FutureExt;
use macros::{AnyFlowNode, SimpleNode, AnyFlowNodeWithParams};
use redis::Commands;
use redis::Connection;
use redis::RedisResult;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::any::Any;
use std::fs;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::Builder;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use axum::{
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
    Json, Router,
};
use std::net::SocketAddr;

const REDIS_ADDR: &str = "redis://127.0.0.1/";

struct Req {
    id: i32,
}

#[derive(Serialize)]
struct Res {
    elems: HashMap<String, i32>
}

#[derive(Deserialize)]
struct Val {
    key: String,
}

#[AnyFlowNodeWithParams]
fn redis_op(graph_args: Arc<Req>, p: Val, input: Arc<OpResults>) -> OpResult {
    let count: i32 = input
        .get::<Mutex<Connection>>(0)
        .unwrap()
        .lock()
        .unwrap()
        .get(&p.key)
        .unwrap();
    OpResult::ok(count)
}

#[AnyFlowNode]
fn redis_con_op(graph_args: Arc<Req>, input: Arc<OpResults>) -> OpResult {
    let mut con: Mutex<Connection> = Mutex::new(
        redis::Client::open(REDIS_ADDR)
            .unwrap()
            .get_connection()
            .unwrap(),
    );
    OpResult::ok(con)
}

#[AnyFlowNodeWithParams]
fn redis_set_op(graph_args: Arc<Req>, p: Val, input: Arc<OpResults>) -> OpResult {
    let prev = input.get::<i32>(0).unwrap();
    input
        .get::<Mutex<Connection>>(0)
        .unwrap()
        .lock()
        .unwrap()
        .set::<&str, i32, i32>(&p.key, prev.clone())
        .unwrap();
    OpResult::ok((p.key.to_string(), prev.clone()))
}

#[AnyFlowNode]
fn pack_op(graph_args: Arc<Req>, input: Arc<OpResults>) -> OpResult {
    let value_pairs = input.mget::<(String, i32)>().unwrap();
    let res = Res {
        elems: value_pairs.iter().map(|(x,y)| (x.clone(), y.clone())).collect::<HashMap<String, i32>>()
    };
    OpResult::ok(res)
}

#[SimpleNode]
fn simple_op(graph_args: Arc<Req>, p: Val, input: i32) -> OpResult {
    
    OpResult::ok(())
}

async fn service() {
    let mut dag = anyflow::dag::Flow::<i32, Req>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    dag.multi_async_register(resgiter_node![redis_set_op, pack_op, simple_op, redis_op]);
    let rt = Runtime::new().unwrap();
    let my_dag = dag.make_flow(Arc::new(Req { id: 1 }));
    rt.block_on(my_dag);
}


#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(service));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}