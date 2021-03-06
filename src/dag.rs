use async_recursion::async_recursion;
use async_std;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::FutureExt;
use futures::future::Shared;
use futures::Future;
use futures::StreamExt;
use futures::{future::BoxFuture, ready};

use pin_project::pin_project;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use std::time::SystemTime;
use tower_service::Service;

#[async_trait]
pub trait AnyHandler {
    type Req: Send + Sync;
    fn config_generate(input: Box<RawValue>) -> Arc<dyn Send + Any + Sync>;

    async fn async_calc2(
        _graph_args: Self::Req,
        params: Arc<dyn Any + Send + Sync>,
        input: Arc<OpResults>,
    ) -> OpResult;
}

pub struct HandlerInfo {
    pub name: &'static str,
    pub method_type: HandlerType,
    pub has_config: bool,
}

pub enum HandlerType {
    Async,
    Sync,
}

#[derive(Deserialize, Default)]
pub struct EmptyPlaceHolder {}

#[derive(Debug, Clone)]
pub enum OpResult {
    Ok(Arc<dyn Any + std::marker::Send + std::marker::Sync>),
    Err(String),
    None,
}

unsafe impl Send for OpResult {}
unsafe impl Sync for OpResult {}

#[derive(Debug)]
pub struct OpResults {
    inner: Vec<Arc<OpResult>>,
}

impl OpResults {
    pub fn get<T: Any>(&self, idx: usize) -> Result<&T, &'static str> {
        if idx >= self.inner.len() {
            Err("out of index")
        } else {
            self.inner[idx].get::<T>()
        }
    }

    pub fn mget<T: Any>(&self) -> Result<Vec<&T>, &'static str> {
        let mut ret = Vec::with_capacity(self.len());
        for idx in 0..self.inner.len() {
            match self.get::<T>(idx) {
                Ok(val) => ret.push(val),
                Err(e) => return Err(e),
            }
        }
        Ok(ret)
    }

    pub fn is_err(&self, idx: usize) -> bool {
        if idx >= self.inner.len() {
            false
        } else {
            self.inner[idx].is_err()
        }
    }

    pub fn get_err(&self, idx: usize) -> Option<&str> {
        if idx >= self.inner.len() {
            None
        } else {
            self.inner[idx].get_err()
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn get_arc(
        &self,
        idx: usize,
    ) -> Option<Arc<dyn Any + std::marker::Send + std::marker::Sync>> {
        if idx >= self.inner.len() {
            None
        } else {
            self.inner[idx].get_arc()
        }
    }

    pub fn at(&self, idx: usize) -> Option<&OpResult> {
        if idx >= self.inner.len() {
            None
        } else {
            Some(&self.inner[idx])
        }
    }
}

impl OpResult {
    pub fn get<T: Any>(&self) -> Result<&T, &'static str> {
        match &self {
            OpResult::Ok(val) => match val.downcast_ref::<T>() {
                Some(val) => Ok(val),
                None => Err("invalid type"),
            },
            OpResult::Err(_e) => Err("is a error"),
            OpResult::None => Err("value is none"),
        }
    }

    pub fn is_err(&self) -> bool {
        match &self {
            OpResult::Ok(_) => false,
            OpResult::Err(_) => true,
            OpResult::None => false,
        }
    }

    pub fn get_err(&self) -> Option<&str> {
        match &self {
            OpResult::Ok(_) => None,
            OpResult::Err(e) => Some(e),
            OpResult::None => None,
        }
    }

    pub fn ok<T: Any + Send + Sync>(t: T) -> OpResult {
        OpResult::Ok(Arc::new(t))
    }

    fn get_arc(&self) -> Option<Arc<dyn Any + std::marker::Send + std::marker::Sync>> {
        match &self {
            OpResult::Ok(val) => Some(Arc::clone(val)),
            OpResult::Err(_e) => None,
            OpResult::None => None,
        }
    }
}

impl Default for OpResult {
    fn default() -> Self {
        OpResult::None
    }
}

#[derive(Deserialize, Default, Debug, Clone)]
struct NodeConfig {
    name: String,
    node: String,
    deps: Vec<String>,
    #[serde(default)]
    params: Box<RawValue>,
    #[serde(default)]
    necessary: bool,
    #[serde(default)]
    cachable: bool,
}

#[derive(Deserialize, Default, Debug, Clone)]
pub struct DAGConfig {
    nodes: Vec<NodeConfig>,
}

#[derive(Debug, Clone)]
pub struct DAGNode {
    node_config: NodeConfig,
    prevs: Vec<String>,
    nexts: Vec<String>,
}

#[derive(Clone)]
pub struct Flow<T: Default + Sync + Send, E: Send + Sync> {
    nodes: HashMap<String, Box<DAGNode>>,

    // global configures
    timeout: Duration,
    pre: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a Arc<OpResults>) -> T + Send + Sync>,
    post: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a Arc<OpResults>, &T) + Send + Sync>,
    timeout_cb: Arc<dyn for<'b> Fn(Arc<DAGNode>, &'b Arc<OpResults>) + Send + Sync>,
    failure_cb: Arc<dyn for<'a> Fn(Arc<DAGNode>, &'a Arc<OpResults>, &'a OpResult) + Send + Sync>,

    // register
    node_mapping: HashMap<
        String,
        Arc<dyn for<'a> Fn(&'a Arc<E>, &'a Box<RawValue>, &'a OpResults) -> OpResult + Send + Sync>,
    >,

    async_node_mapping: HashMap<
        String,
        Arc<
            Mutex<
                dyn Service<
                        (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
                        Response = OpResult,
                        Error = &'static str,
                        Future = AsyncHandlerFuture,
                    > + Send
                    + Sync,
            >,
        >,
    >,

    // cache
    cached_repo: Arc<dashmap::DashMap<String, (Arc<OpResult>, SystemTime)>>,

    // config cache
    node_config_repo: HashMap<String, Arc<dyn Any + std::marker::Send + Sync>>,
    has_node_config_repo: HashMap<String, bool>,
    node_config_generator_repo: HashMap<
        String,
        Arc<dyn Fn(Box<RawValue>) -> Arc<dyn Any + std::marker::Send + Sync> + Send + Sync>,
    >,
}

impl<T: 'static + Default + Send + Sync, E: 'static + Send + Sync> Default for Flow<T, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: 'static + Default + Send + Sync, E: 'static + Send + Sync> Flow<T, E> {
    pub fn new<'a>() -> Flow<T, E> {
        Flow {
            nodes: HashMap::new(),
            timeout: Duration::from_secs(5),
            pre: Arc::new(|_, _| T::default()), // placeholder
            post: Arc::new(|_, _, _| {}),       // placeholder
            timeout_cb: Arc::new(|_, _| {}),    // placeholder
            failure_cb: Arc::new(|_, _, _| {}), // placeholder
            node_mapping: HashMap::new(),
            async_node_mapping: HashMap::new(),
            cached_repo: Arc::new(DashMap::new()),
            node_config_repo: HashMap::new(),
            node_config_generator_repo: HashMap::new(),
            has_node_config_repo: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        node_name: &str,
        handle: Arc<
            dyn for<'a> Fn(&'a Arc<E>, &'a Box<RawValue>, &OpResults) -> OpResult + Send + Sync,
        >,
    ) {
        self.node_mapping
            .insert(node_name.to_string(), Arc::clone(&handle));
    }

    pub fn async_register<H>(&mut self, node_name: &str, handler: H)
    where
        H: AsyncHandler<E>,
    {
        self.async_node_mapping.insert(
            node_name.to_string(),
            Arc::new(Mutex::new(Flow::<T, E>::wrap(handler))),
        );
    }

    pub fn multi_async_register<H>(
        &mut self,
        handlers_ganerator: &dyn Fn() -> Vec<(
            &'static str,
            H,
            Arc<dyn Fn(Box<RawValue>) -> Arc<(dyn Any + std::marker::Send + Sync)> + Send + Sync>,
            bool,
        )>,
    ) where
        H: AsyncHandler<E>,
    {
        for pair in handlers_ganerator() {
            self.async_node_mapping.insert(
                pair.0.to_string(),
                Arc::new(Mutex::new(Flow::<T, E>::wrap(pair.1))),
            );
            self.node_config_generator_repo
                .insert(pair.0.to_string(), pair.2);
            self.has_node_config_repo.insert(pair.0.to_string(), pair.3);
        }
    }

    fn wrap<H>(handler: H) -> AsyncContainer<H>
    where
        H: AsyncHandler<E>,
    {
        AsyncContainer { handler }
    }

    pub fn init(&mut self, conf_content: &str) -> Result<(), String> {
        let dag_config: DAGConfig = serde_json::from_str(conf_content).unwrap();
        for node_config in dag_config.nodes.iter() {
            self.nodes.insert(
                node_config.name.clone(),
                Box::new(DAGNode {
                    node_config: node_config.clone(),
                    nexts: Vec::new(),
                    prevs: Vec::new(),
                }),
            );
        }
        for node_config in dag_config.nodes.iter() {
            for dep in node_config.deps.iter() {
                if dep == &node_config.name {
                    return Err(format!("{:?} depend itself", node_config.name));
                }
                if !self.nodes.contains_key(&dep.clone()) {
                    return Err(format!(
                        "{:?}'s dependency {:?} do not exist",
                        node_config.name, dep
                    ));
                }
                self.nodes
                    .get_mut(&node_config.name.clone())
                    .unwrap()
                    .prevs
                    .push(dep.clone());
                self.nodes
                    .get_mut(&dep.clone())
                    .unwrap()
                    .nexts
                    .push(node_config.name.clone());
            }

            if !self.async_node_mapping.contains_key(&node_config.node) {
                return Err(format!("miss key {:?}", node_config.node));
            }

            if *self.has_node_config_repo.get(&node_config.node).unwrap() {
                self.node_config_repo.insert(
                    node_config.name.clone(),
                    self.node_config_generator_repo
                        .get(&node_config.node)
                        .unwrap()(node_config.params.clone()),
                );
            }
        }

        let root_nodes: HashSet<String> = self
            .nodes
            .values()
            .filter(|node| node.prevs.is_empty())
            .map(|node| node.node_config.name.clone())
            .collect();
        for root in root_nodes {
            if !self.check_flow(&mut HashSet::new(), root.to_string()) {
                return Err(("have cycle").to_string());
            }
        }

        Ok(())
    }

    fn check_flow<'a>(&self, path: &mut HashSet<String>, cur: String) -> bool {
        if path.contains(&cur) {
            return false;
        }
        let node = self.nodes.get(&cur).unwrap();
        if node.nexts.is_empty() {
            return true;
        }

        path.insert(cur.clone());
        for next in &node.nexts {
            if !self.check_flow(&mut path.clone(), next.to_string()) {
                return false;
            }
        }
        true
    }

    pub async fn make_flow(&self, args: Arc<E>) -> Vec<Arc<OpResult>> {
        let leaf_nodes: HashSet<String> = self
            .nodes
            .values()
            .filter(|node| node.nexts.is_empty())
            .map(|node| node.node_config.name.clone())
            .collect();

        let have_handled: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let nodes_ptr: Arc<HashMap<String, Arc<DAGNode>>> = Arc::new(
            self.nodes
                .iter()
                .map(|(k, v)| (k.clone(), Arc::new(*v.clone())))
                .collect(),
        );
        let dag_futures_ptr: Arc<
            Mutex<
                HashMap<
                    std::string::String,
                    Shared<
                        Pin<Box<dyn futures::Future<Output = Arc<OpResult>> + std::marker::Send>>,
                    >,
                >,
            >,
        > = Arc::new(Mutex::new(HashMap::new()));
        for leaf in leaf_nodes.iter() {
            let dag_futures_ptr_copy = Arc::clone(&dag_futures_ptr);
            dag_futures_ptr.lock().unwrap().insert(
                leaf.to_string(),
                // entry.boxed().shared()
                Flow::<T, E>::dfs_node(
                    dag_futures_ptr_copy,
                    Arc::clone(&have_handled),
                    Arc::clone(&nodes_ptr),
                    leaf.clone(),
                    Arc::clone(&self.pre),
                    Arc::clone(&self.post),
                    Arc::clone(&self.timeout_cb),
                    Arc::clone(&self.failure_cb),
                    Arc::new(
                        self.node_mapping
                            .iter()
                            .map(|(key, val)| (key.clone(), Arc::clone(val)))
                            .collect(),
                    ),
                    Arc::new(
                        self.async_node_mapping
                            .iter()
                            .map(|(key, val)| (key.clone(), Arc::clone(val)))
                            .collect(),
                    ),
                    Arc::clone(&args),
                    Arc::clone(&self.cached_repo),
                    Arc::new(
                        self.node_config_repo
                            .iter()
                            .map(|(key, val)| (key.clone(), Arc::clone(val)))
                            .collect(),
                    ),
                    Arc::new(
                        self.has_node_config_repo
                            .iter()
                            .map(|(key, val)| (key.clone(), *val))
                            .collect(),
                    ),
                )
                .boxed()
                .shared(),
            );
        }

        let mut leaves: futures::stream::FuturesUnordered<_> = leaf_nodes
            .iter()
            .map(|leaf| dag_futures_ptr.lock().unwrap().get(&*leaf).unwrap().clone())
            .collect();

        let mut results = Vec::with_capacity(leaves.len());

        while let Some(item) = leaves.next().await {
            results.push(item);
        }
        results
    }

    #[async_recursion]
    async fn dfs_node<'a>(
        dag_futures: Arc<
            Mutex<
                HashMap<
                    std::string::String,
                    Shared<
                        Pin<Box<dyn futures::Future<Output = Arc<OpResult>> + std::marker::Send>>,
                    >,
                >,
            >,
        >,
        have_handled: Arc<Mutex<HashSet<String>>>,
        nodes: Arc<HashMap<String, Arc<DAGNode>>>,
        node: String,
        pre_fn: Arc<dyn for<'b> Fn(&'b Arc<E>, &'b Arc<OpResults>) -> T + Send + Sync>,
        post_fn: Arc<dyn for<'b> Fn(&'b Arc<E>, &'b Arc<OpResults>, &T) + Send + Sync>,
        timeout_cb_fn: Arc<dyn for<'b> Fn(Arc<DAGNode>, &'b Arc<OpResults>) + Send + Sync>,
        failure_cb_fn: Arc<
            dyn for<'b> Fn(Arc<DAGNode>, &'b Arc<OpResults>, &'b OpResult) + Send + Sync,
        >,
        node_mapping: Arc<
            HashMap<
                String,
                Arc<
                    dyn for<'b> Fn(&'b Arc<E>, &'b Box<RawValue>, &'b OpResults) -> OpResult
                        + Sync
                        + Send,
                >,
            >,
        >,
        async_node_mapping: Arc<
            HashMap<
                String,
                Arc<
                    Mutex<
                        dyn Service<
                                (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
                                Response = OpResult,
                                Error = &'static str,
                                Future = AsyncHandlerFuture,
                            > + Send
                            + Sync,
                    >,
                >,
            >,
        >,
        args: Arc<E>,
        cached_repo: Arc<dashmap::DashMap<String, (Arc<OpResult>, SystemTime)>>,
        node_config_repo: Arc<HashMap<String, Arc<dyn Any + std::marker::Send + Sync>>>,
        has_node_config_repo: Arc<HashMap<String, bool>>,
    ) -> Arc<OpResult> {
        let mut deps = futures::stream::FuturesOrdered::new();
        if nodes.get(&node).unwrap().prevs.is_empty() {
            deps.push(async { Arc::new(OpResult::default()) }.boxed().shared());
        } else {
            deps = nodes
                .get(&node)
                .unwrap()
                .prevs
                .iter()
                .map(|prev| {
                    if have_handled.lock().unwrap().contains(&prev.to_string()) {
                        dag_futures.lock().unwrap().get(prev).unwrap().clone()
                    } else {
                        let prev_ptr = Arc::new(prev);
                        dag_futures.lock().unwrap().insert(
                            prev.to_string(),
                            Flow::<T, E>::dfs_node(
                                Arc::clone(&dag_futures),
                                Arc::clone(&have_handled),
                                Arc::clone(&nodes),
                                prev_ptr.to_string(),
                                Arc::clone(&pre_fn),
                                Arc::clone(&post_fn),
                                Arc::clone(&timeout_cb_fn),
                                Arc::clone(&failure_cb_fn),
                                Arc::clone(&node_mapping),
                                Arc::clone(&async_node_mapping),
                                Arc::clone(&args),
                                Arc::clone(&cached_repo),
                                Arc::clone(&node_config_repo),
                                Arc::clone(&has_node_config_repo),
                            )
                            .boxed()
                            .shared(),
                        );
                        have_handled.lock().unwrap().insert(prev_ptr.to_string());
                        dag_futures.lock().unwrap().get(prev).unwrap().clone()
                    }
                })
                .collect()
        };

        let mut collector = Vec::with_capacity(deps.len());
        while let Some(item) = deps.next().await {
            collector.push(item);
        }

        let prev_results = Arc::new(OpResults { inner: collector });

        let params_ptr = node_config_repo.get(&node).unwrap();
        let async_handle_fn = Arc::clone(
            async_node_mapping
                .get(&nodes.get(&node).unwrap().node_config.node)
                .unwrap(),
        );
        let arg_ptr = Arc::clone(&args);

        let pre_result: T = pre_fn(&arg_ptr, &prev_results);

        let now = SystemTime::now();
        let res = if nodes.get(&node).unwrap().node_config.cachable
            && cached_repo.contains_key(&node)
            && now
                .duration_since(cached_repo.get(&node).unwrap().1)
                .unwrap()
                > Duration::from_secs(60)
        {
            Arc::clone(&cached_repo.get(&node).unwrap().0)
        } else {
            let r = match async_std::future::timeout(Duration::from_secs(10), async {
                let v = async_handle_fn.lock().unwrap().call((
                    Arc::clone(&arg_ptr),
                    params_ptr.clone(),
                    Arc::clone(&prev_results),
                ));
                v.await.unwrap()
            })
            .await
            {
                Err(_) => {
                    timeout_cb_fn(Arc::clone(nodes.get(&node).unwrap()), &prev_results);

                    Arc::new(OpResult::Err("timeout".to_string()))
                }
                Ok(val) => Arc::new(val),
            };
            if r.is_err() {
                // failure_cb_fn(Arc::clone(nodes.get(&node).unwrap()), &prev_res, &r);
            } else if nodes.get(&node).unwrap().node_config.cachable {
                cached_repo.insert(node.clone(), (Arc::clone(&r), SystemTime::now()));
            }
            r
        };

        post_fn(&arg_ptr, &prev_results, &pre_result);
        if res.is_err() && nodes.get(&node).unwrap().node_config.necessary {
            Arc::new(OpResult::default())
        } else {
            res
        }
    }
}

#[async_trait]
pub trait AsyncHandler<E>: Clone + Sync + Send + Sized + 'static {
    async fn call(self, q: Arc<E>, e: Arc<dyn Any + Sync + Send>, w: Arc<OpResults>) -> OpResult;
}

#[async_trait]
impl<F, Fut, E> AsyncHandler<E> for F
where
    F: FnOnce(Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = OpResult> + Send,
    E: Send + Sync + 'static,
{
    async fn call(self, q: Arc<E>, e: Arc<dyn Any + Sync + Send>, w: Arc<OpResults>) -> OpResult {
        self(q, e, w).await
    }
}

#[derive(Clone, Copy)]
struct AsyncContainer<B> {
    handler: B,
    // _marker: PhantomData,
}

unsafe impl<B> Send for AsyncContainer<B> {}
unsafe impl<B> Sync for AsyncContainer<B> {}

impl<B, E> Service<(Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>)> for AsyncContainer<B>
where
    B: AsyncHandler<E>,
{
    type Response = OpResult;
    type Error = &'static str;
    type Future = AsyncHandlerFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, q: (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>)) -> Self::Future {
        let ft = AsyncHandler::call(self.handler.clone(), q.0, q.1, q.2);
        AsyncHandlerFuture { inner: ft }
    }
}

#[async_trait]
pub trait Handler: Clone + Send + Sized + 'static {
    async fn call(self, req: i32) -> i32;
}

#[async_trait]
impl<F, Fut> Handler for F
where
    F: FnOnce(i32) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = i32> + Send,
{
    async fn call(self, req: i32) -> i32 {
        self(req).await
    }
}

#[pin_project]
pub struct AsyncHandlerFuture {
    #[pin]
    inner: BoxFuture<'static, OpResult>,
}

impl Future for AsyncHandlerFuture {
    type Output = Result<OpResult, &'static str>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let cur = ready!(this.inner.poll(cx));
        Poll::Ready(Ok(cur))
    }
}
