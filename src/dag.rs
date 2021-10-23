use async_recursion::async_recursion;

use futures::future::FutureExt;
use futures::future::Shared;

use futures::StreamExt;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::any::Any;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::pin::Pin;
use tokio::time::timeout;

use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum FlowResult {
    Ok(HashMap<String, Arc<dyn Any + std::marker::Send>>),
    Err(&'static str),
}

unsafe impl Send for FlowResult {}
unsafe impl Sync for FlowResult {}

impl FlowResult {
    pub fn new() -> FlowResult {
        FlowResult::Ok(HashMap::new())
    }
    pub fn get<T: Any + Debug + Clone>(&self, key: &str) -> Result<&T, &'static str> {
        match self {
            FlowResult::Ok(kv) => match kv.get(&key.to_string()) {
                Some(val) => match val.downcast_ref::<T>() {
                    Some(val) => Ok(val),
                    None => Err("type error"),
                },
                None => Err("miss key"),
            },
            FlowResult::Err(e) => Err(e),
        }
    }
    pub fn set<T: Any + Debug + Clone + std::marker::Send>(
        &mut self,
        key: &str,
        val: T,
    ) -> &FlowResult {
        match self {
            FlowResult::Ok(kv) => {
                kv.insert(key.to_string(), Arc::new(val));
                self
            }
            FlowResult::Err(e) => self,
        }
    }
    fn merge(&self, other: &FlowResult) -> FlowResult {
        match self {
            FlowResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                match other {
                    FlowResult::Ok(other_kv) => new_kv.extend(other_kv.clone()),
                    FlowResult::Err(_e) => {}
                }
                FlowResult::Ok(new_kv)
            }
            FlowResult::Err(e) => FlowResult::Err(e),
        }
    }

    pub fn get_map<T: Any + Debug + Clone + std::marker::Send>(
        &self,
    ) -> Result<HashMap<String, &T>, &'static str> {
        match self {
            FlowResult::Ok(kv) => {
                let mut ret = HashMap::new();
                for (k, v) in kv {
                    match v.downcast_ref::<T>() {
                        Some(val) => ret.insert(k.clone(), val),
                        None => return Err("type assert failed"),
                    };
                }
                Ok(ret)
            }
            FlowResult::Err(e) => Err(e),
        }
    }
}

#[derive(Deserialize, Default, Debug, Clone)]
struct NodeConfig {
    name: String,
    node: String,
    deps: Vec<String>,
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
    prevs: HashSet<String>,
    nexts: HashSet<String>,
}

fn handle_wrapper<'a, E: Send + Sync>(
    _graph_args: &'a Arc<E>,
    _input: Arc<FlowResult>,
    _params: Box<RawValue>,
) -> FlowResult {
    let mut t = FlowResult::new();
    t.set("xxx", DAGConfig::default());
    let _c = t.get::<DAGConfig>("xxx");
    t
}

pub struct Flow<T: Default + Sync + Send, E: Send + Sync> {
    nodes: HashMap<String, Box<DAGNode>>,

    // global configures
    timeout: Duration,
    pre: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a FlowResult) -> T + Send + Sync>,
    post: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a FlowResult, &T) + Send + Sync>,
    timeout_cb: Arc<dyn for<'a> Fn() + Send + Sync>,
    failure_cb: Arc<dyn for<'a> Fn(&'a FlowResult) + Send + Sync>,

    // register
    node_mapping: HashMap<
        String,
        Arc<dyn for<'a> Fn(&'a Arc<E>, Arc<FlowResult>, Box<RawValue>) -> FlowResult + Sync + Send>,
    >,
}

impl<T: 'static + Default + Send + Sync, E: 'static + Send + Sync> Flow<T, E> {
    pub fn new() -> Flow<T, E> {
        Flow {
            nodes: HashMap::new(),
            timeout: Duration::from_secs(5),
            pre: Arc::new(|_a, _b| T::default()),
            post: Arc::new(|_a, _b, _c| {}),
            timeout_cb: Arc::new(|| {}),
            failure_cb: Arc::new(|_a| {}),
            node_mapping: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        node_name: &str,
        handle: Arc<
            dyn for<'a> Fn(&'a Arc<E>, Arc<FlowResult>, Box<RawValue>) -> FlowResult + Sync + Send,
        >,
    ) {
        self.node_mapping
            .insert(node_name.to_string(), Arc::clone(&handle));
    }

    pub fn registers(
        &mut self,
        _nodes: &[(
            &str,
            &(dyn for<'a> Fn(&'a Arc<E>, Arc<FlowResult>) -> FlowResult + Sync + Send),
        )],
    ) {
    }

    pub fn init(&mut self, conf_content: &str) -> Result<(), String> {
        let dag_config: DAGConfig = serde_json::from_str(conf_content).unwrap();
        for node_config in dag_config.nodes.iter() {
            self.nodes.insert(
                node_config.name.clone(),
                Box::new(DAGNode {
                    node_config: node_config.clone(),
                    nexts: HashSet::new(),
                    prevs: HashSet::new(),
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
                    .insert(dep.clone());
                self.nodes
                    .get_mut(&dep.clone())
                    .unwrap()
                    .nexts
                    .insert(node_config.name.clone());
            }
        }

        Ok(())
    }

    pub async fn make_flow(&self, args: Arc<E>) -> Vec<FlowResult> {
        let leaf_nodes: HashSet<String> = self
            .nodes
            .values()
            .filter(|node| node.nexts.is_empty())
            .map(|node| node.node_config.name.clone())
            .collect();

        // let mut dag_futures: HashMap<_, _> = self
        //     .nodes
        //     .lock()
        //     .unwrap()
        //     .iter()
        //     .map(|(node_name, _)| {
        //         let entry = async move {
        //             println!("oihiohiohoiho {:?}", node_name.clone());
        //             FlowResult::new()
        //         };
        //         (node_name.clone(), Box::new(entry.boxed().shared()))
        //     })
        //     .collect();

        let have_handled: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let nodes_ptr: Arc<HashMap<String, Box<DAGNode>>> = Arc::new(
            self.nodes
                .iter()
                .map(|(k, v)| (k.clone(), Box::new(*v.clone())))
                .collect(),
        );
        let _n: Arc<Vec<Box<String>>> = Arc::new(
            self.nodes
                .iter()
                .map(|(key, _val)| Box::new(key.clone()))
                .collect(),
        );
        // let mut dag_futures_ptr: Arc<Mutex<HashMap<_, _>>> = Arc::new(Mutex::new(
        //     n.iter()
        //         .map(|node_name| {
        //             let entry = async move {
        //                 println!("oihiohiohoiho {:?}", *node_name.clone());
        //                 FlowResult::new()
        //             };
        //             (*node_name.clone(), entry.boxed().shared())
        //         })
        //         .collect(),
        // ));
        let dag_futures_ptr: Arc<
            Mutex<
                HashMap<
                    std::string::String,
                    Shared<Pin<Box<dyn futures::Future<Output = FlowResult> + std::marker::Send>>>,
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
                    Arc::clone(&args),
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
                    Shared<Pin<Box<dyn futures::Future<Output = FlowResult> + std::marker::Send>>>,
                >,
            >,
        >,
        have_handled: Arc<Mutex<HashSet<String>>>,
        nodes: Arc<HashMap<String, Box<DAGNode>>>,
        node: String,
        pre_fn: Arc<dyn for<'b> Fn(&'b Arc<E>, &'b FlowResult) -> T + Send + Sync>,
        post_fn: Arc<dyn for<'b> Fn(&'b Arc<E>, &'b FlowResult, &T) + Send + Sync>,
        timeout_cb_fn: Arc<dyn Fn() + Send + Sync>,
        failure_cb_fn: Arc<dyn for<'b> Fn(&'b FlowResult) + Send + Sync>,
        node_mapping: Arc<
            HashMap<
                String,
                Arc<
                    dyn for<'b> Fn(&'b Arc<E>, Arc<FlowResult>, Box<RawValue>) -> FlowResult
                        + Sync
                        + Send,
                >,
            >,
        >,
        args: Arc<E>,
    ) -> FlowResult {
        // println!("xxx {:?}", node);
        let mut deps = futures::stream::FuturesUnordered::new();
        if nodes.get(&node).unwrap().prevs.is_empty() {
            deps.push(async { FlowResult::new() }.boxed().shared());
        } else {
            for prev in nodes.get(&node).unwrap().prevs.iter() {
                let prev_ptr = Arc::new(prev);
                let dag_futures_ptr = Arc::clone(&dag_futures);
                let have_handled_ptr = Arc::clone(&have_handled);
                let nodes_ptr = Arc::clone(&nodes);
                let pre_ptr = Arc::clone(&pre_fn);
                let post_ptr = Arc::clone(&post_fn);
                let timeout_cb_ptr = Arc::clone(&timeout_cb_fn);
                let failure_cb_ptr = Arc::clone(&failure_cb_fn);
                let node_mapping_ptr = Arc::clone(&node_mapping);
                let arg_ptr = Arc::clone(&args);

                if !have_handled.lock().unwrap().contains(&prev.to_string()) {
                    dag_futures.lock().unwrap().insert(
                        prev.to_string(),
                        Flow::<T, E>::dfs_node(
                            dag_futures_ptr,
                            have_handled_ptr,
                            nodes_ptr,
                            prev_ptr.to_string(),
                            pre_ptr,
                            post_ptr,
                            timeout_cb_ptr,
                            failure_cb_ptr,
                            node_mapping_ptr,
                            arg_ptr,
                        )
                        .boxed()
                        .shared(),
                    );
                    have_handled.lock().unwrap().insert(prev_ptr.to_string());
                }
                deps.push(dag_futures.lock().unwrap().get(prev).unwrap().clone());
            }
        }

        let params_ptr = nodes.get(&node).unwrap().node_config.params.clone();
        let handle_fn = Arc::clone(
            node_mapping
                .get(&nodes.get(&node).unwrap().node_config.node)
                .unwrap(),
        );
        let arg_ptr = Arc::clone(&args);

        let mut results = Vec::with_capacity(deps.len());
        while let Some(item) = deps.next().await {
            results.push(item);
        }

        let prev_res = Arc::new(results.iter().fold(FlowResult::new(), |a, b| a.merge(b))); //TODO: process
        let pre_result: T = pre_fn(&arg_ptr, &prev_res);
        let res = match timeout(Duration::from_secs(1), async {
            handle_fn(&arg_ptr, prev_res.clone(), params_ptr)
        })
        .await
        {
            Err(_) => FlowResult::Err("timeout"),
            Ok(val) => val,
        };
        post_fn(&arg_ptr, &prev_res, &pre_result);
        res
    }
}

fn demo() {
    let _dag = Flow::<i32, i32>::new().register("handle_wrapper", Arc::new(handle_wrapper));
}
