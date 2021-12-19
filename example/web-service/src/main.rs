#[macro_use]
extern crate lazy_static;
extern crate pretty_env_logger;
extern crate redis;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use anyflow::dag::OpResults;
use anyflow::resgiter_node;
use anyflow::AnyHandler;
use anyflow::EmptyPlaceHolder;
use anyflow::HandlerInfo;
use anyflow::OpResult;
use async_trait::async_trait;
// use axum::{
//     extract::{Extension, FromRequest, RequestParts},
//     http::StatusCode,
//     routing::get,
//     AddExtensionLayer, Router,
// };
use futures::future::FutureExt;
use log::{info, warn};
use macros::{AnyFlowNode, AnyFlowNodeWithParams, SimpleNode};
use redis::Commands;
use redis::Connection;
use redis::RedisResult;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::Builder;
use tokio::runtime::Runtime;

const REDIS_ADDR: &str = "redis://127.0.0.1:6379";

struct Req {
    id: i32,
}

#[derive(Serialize, Clone)]
struct Res {
    elems: HashMap<String, i32>,
}

#[derive(Deserialize)]
struct Val {
    key: String,
    name: String,
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
    input.at(0).unwrap().clone()
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
    input
        .get::<Mutex<Connection>>(0)
        .unwrap()
        .lock()
        .unwrap()
        .set::<&str, i32, String>(&p.key, 1)
        .unwrap();
    OpResult::ok((p.key.to_string(), 1))
}

#[AnyFlowNode]
fn pack_op(graph_args: Arc<Req>, input: Arc<OpResults>) -> OpResult {
    let value_pairs = input.mget::<(String, i32)>().unwrap();
    let res = Res {
        elems: value_pairs
            .iter()
            .map(|(x, y)| (x.clone(), y.clone()))
            .collect::<HashMap<String, i32>>(),
    };
    OpResult::ok(res)
}

#[SimpleNode]
fn simple_op(graph_args: Arc<Req>, p: Val, input: i32) -> OpResult {
    OpResult::ok(())
}

async fn service() -> impl Responder {
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    let mut dag = anyflow::dag::Flow::<i32, Req>::new();
    dag.multi_async_register(resgiter_node![
        redis_set_op,
        pack_op,
        simple_op,
        redis_op,
        redis_con_op
    ]);
    dag.init(&data).unwrap();
    for _ in 0..10000 {
        let dag_result = dag.make_flow(Arc::new(Req { id: 2 })).await;
        let res = dag_result[0].get::<Res>().unwrap();
    }

    let dag_result = dag.make_flow(Arc::new(Req { id: 2 })).await;
    let res = dag_result[0].get::<Res>().unwrap();
    web::Json(res.clone())
}

fn main() {
    // pretty_env_logger::init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(service());
}

// #[tokio::main]
// async fn main() {
//     let app = Router::new()
//         .route("/", get(service));
//     let addr = SocketAddr::from(([127, 0, 0, 1], 7777));
//     axum::Server::bind(&addr)
//         .serve(app.into_make_service())
//         .await
//         .unwrap();
// }

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     pretty_env_logger::init();
//     HttpServer::new(|| App::new().route("/", web::get().to(service)))
//         .bind(("127.0.0.1", 8080))?
//         .run()
//         .await
// }
