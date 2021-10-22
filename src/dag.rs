use async_recursion::async_recursion;
use futures::future::join_all;
use futures::future::FutureExt;
use futures::future::Shared;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::any::Any;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone, Debug)]
pub enum NodeResult {
    Ok(HashMap<String, Arc<dyn Any + std::marker::Send>>),
    Err(&'static str),
}

unsafe impl Send for NodeResult {}
unsafe impl Sync for NodeResult {}

impl NodeResult {
    pub fn new() -> NodeResult {
        NodeResult::Ok(HashMap::new())
    }
    pub fn get<T: Any + Debug + Clone>(&self, key: &str) -> Option<T> {
        match self {
            NodeResult::Ok(kv) => match kv.get(&key.to_string()) {
                Some(val) => val.downcast_ref::<T>().cloned(),
                None => None,
            },
            NodeResult::Err(_) => None,
        }
    }
    pub fn set<T: Any + Debug + Clone + std::marker::Send>(
        &self,
        key: &str,
        val: &T,
    ) -> NodeResult {
        match self {
            NodeResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                new_kv.insert(key.to_string(), Arc::new(val.clone()));
                NodeResult::Ok(new_kv)
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }
    fn merge(&self, other: &NodeResult) -> NodeResult {
        match self {
            NodeResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                match other {
                    NodeResult::Ok(other_kv) => new_kv.extend(other_kv.clone()),
                    NodeResult::Err(_e) => {}
                }
                NodeResult::Ok(new_kv)
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }

    fn get_map<T: Any + Debug + Clone + std::marker::Send>(&self) -> Option<HashMap<String, T>> {
        match self {
            NodeResult::Ok(kv) => {
                let mut ret = HashMap::new();
                for (k, v) in kv {
                    match v.downcast_ref::<T>().cloned() {
                        Some(val) => ret.insert(k.clone(), val.clone()),
                        None => None,
                    };
                }
                Some(ret)
            }
            NodeResult::Err(_) => None,
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
    _input: Arc<NodeResult>,
    _params: Box<RawValue>,
) -> NodeResult {
    let t = NodeResult::new();
    t.set("xxx", &DAGConfig::default());
    let _c = t.get::<DAGConfig>("xxx");
    t
}

pub struct Flow<T: Default + Sync + Send, E: Send + Sync> {
    nodes: HashMap<String, Box<DAGNode>>,

    // global configures
    timeout: Duration,
    pre: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a NodeResult) -> T + Send + Sync>,
    post: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a NodeResult, &T) + Send + Sync>,
    timeout_cb: Arc<dyn for<'a> Fn() + Send + Sync>,
    failure_cb: Arc<dyn for<'a> Fn(&'a NodeResult)>,

    // register
    node_mapping: HashMap<
        String,
        Arc<dyn for<'a> Fn(&'a Arc<E>, Arc<NodeResult>, Box<RawValue>) -> NodeResult + Sync + Send>,
    >,
}

// impl<T: Default + Sync + Send, E: Send + Sync> Clone for Flow<T, E> {
//     fn clone(&self) -> Flow<T, E> {
//         Flow {
//             nodes: Arc::clone(&self.nodes),
//             timeout: Duration::from_secs(5),
//             pre: Arc::clone(&self.pre),
//             post: Arc::clone(&self.post),
//             timeout_cb: Arc::clone(&self.timeout_cb),
//             failure_cb: Arc::clone(&self.failure_cb),
//             node_mapping: Arc::clone(&self.node_mapping),
//         }
//     }
// }

// impl<T: Default + Sync + Send, E: Send + Sync> Copy for Flow<T, E> {}

impl<T: Default + Send + Sync, E: Send + Sync> Flow<T, E> {
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
            dyn for<'a> Fn(&'a Arc<E>, Arc<NodeResult>, Box<RawValue>) -> NodeResult + Sync + Send,
        >,
    ) {
        self.node_mapping
            .insert(node_name.to_string(), Arc::clone(&handle));
    }

    pub fn registers(
        &mut self,
        _nodes: &[(
            &str,
            &(dyn for<'a> Fn(&'a Arc<E>, Arc<NodeResult>) -> NodeResult + Sync + Send),
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

        // println!("data: {:?}", self.nodes);

        Ok(())
    }

    pub async fn make_flow(&self, args: Arc<E>) -> Vec<NodeResult> {
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
        //             NodeResult::new()
        //         };
        //         (node_name.clone(), Box::new(entry.boxed().shared()))
        //     })
        //     .collect();

        let mut have_handled: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let mut nodes_ptr: Arc<HashMap<String, Box<DAGNode>>> = Arc::new(
            self.nodes
                .iter()
                .map(|(k, v)| (k.clone(), Box::new(*v.clone())))
                .collect(),
        );
        let n: Arc<Vec<Box<String>>> = Arc::new(
            self.nodes
                .iter()
                .map(|(key, val)| Box::new(key.clone()))
                .collect(),
        );
        // let mut dag_futures_ptr: Arc<Mutex<HashMap<_, _>>> = Arc::new(Mutex::new(
        //     n.iter()
        //         .map(|node_name| {
        //             let entry = async move {
        //                 println!("oihiohiohoiho {:?}", *node_name.clone());
        //                 NodeResult::new()
        //             };
        //             (*node_name.clone(), entry.boxed().shared())
        //         })
        //         .collect(),
        // ));
        let mut dag_futures_ptr: Arc<
            Mutex<
                HashMap<
                    std::string::String,
                    Shared<Pin<Box<dyn futures::Future<Output = NodeResult> + std::marker::Send>>>,
                >,
            >,
        > = Arc::new(Mutex::new(HashMap::new()));
        for leaf in leaf_nodes.iter() {
            let dag_futures_ptr_copy = Arc::clone(&dag_futures_ptr);
            // let entry = async move {
            //     println!("oihiohiohoiho {:?}", leaf.clone());
            //     NodeResult::new()
            // };
            dag_futures_ptr.lock().unwrap().insert(
                leaf.to_string(),
                // entry.boxed().shared()
                Flow::<T, E>::dfs_node(
                    dag_futures_ptr_copy,
                    Arc::clone(&have_handled),
                    Arc::clone(&nodes_ptr),
                    leaf.clone(),
                )
                .boxed()
                .shared(),
            );
        }

        // TODO: make it DFS
        // for (node_name, node) in self.nodes.lock().unwrap().iter() {
        //     println!(
        //         "{:?} {:?}",
        //         node_name,
        //         self.nodes.lock().unwrap().get(node_name).unwrap().prevs
        //     );
        //     let mut deps: FuturesUnordered<_> = self
        //         .nodes
        //         .lock()
        //         .unwrap()
        //         .get(node_name)
        //         .unwrap()
        //         .prevs
        //         .iter()
        //         .map(|dep| dag_futures.get(dep).unwrap().clone())
        //         .collect();
        //     // println!("node {:?} node_name: {:?}", node_name, deps.len());

        //     let arg_ptr = Arc::clone(&args);
        //     let params_ptr = node.node_config.params.clone();
        //     let pre_fn = Arc::clone(&self.pre);
        //     let post_fn = Arc::clone(&self.post);
        //     let handle_fn = Arc::clone(
        //         self.node_mapping
        //             .lock()
        //             .unwrap()
        //             .get(&node.node_config.node)
        //             .unwrap(),
        //     );
        //     *dag_futures.get_mut(node_name).unwrap() = Box::new(
        //         join_all(deps)
        //             .then(|results| async move {
        //                 // async move {
        //                 // let mut results = Vec::with_capacity(deps.len());
        //                 // while let Some(item) = deps.next().await {
        //                 //     println!("xxxx {:?}", item);
        //                 //     results.push(item)
        //                 // }
        //                 // println!("results {:?} {:?}", results.len(), deps.len());
        //                 // let params = node_instance.deserialize(&params_ptr);
        //                 let prev_res =
        //                     Arc::new(results.iter().fold(NodeResult::new(), |a, b| a.merge(b))); //TODO: process
        //                 let pre_result: T = pre_fn(&arg_ptr, &prev_res);
        //                 let res = match timeout(Duration::from_secs(1), async {
        //                     handle_fn(&arg_ptr, prev_res.clone(), params_ptr)
        //                 })
        //                 .await
        //                 {
        //                     Err(_) => NodeResult::Err("timeout"),
        //                     Ok(val) => val,
        //                 };
        //                 post_fn(&arg_ptr, &prev_res, &pre_result);
        //                 res
        //             })
        //             .boxed()
        //             .shared(),
        //     );
        // }

        // let mut leaves: FuturesUnordered<_> = leaf_nodes
        //     .iter()
        //     .map(|x| dag_futures.get(x).unwrap().clone())
        //     .collect();
        // let mut results = Vec::with_capacity(leaves.len());
        // while let Some(item) = leaves.next().await {
        //     results.push(item)
        // }
        let mut leaves = Vec::new();
        for leaf in leaf_nodes {
            leaves.push(dag_futures_ptr.lock().unwrap().get(&leaf).unwrap().clone());
            println!("leave {:?}", leaf);
        }
        let results = join_all(leaves).await;
        results
    }

    #[async_recursion]
    async fn dfs_node<'a>(
        mut dag_futures: Arc<
            Mutex<
                HashMap<
                    std::string::String,
                    Shared<Pin<Box<dyn futures::Future<Output = NodeResult> + std::marker::Send>>>,
                >,
            >,
        >,
        mut have_handled: Arc<Mutex<HashSet<String>>>,
        nodes: Arc<HashMap<String, Box<DAGNode>>>,
        node: String,
    ) -> NodeResult {
        println!("xxx {:?}", node);
        if nodes.get(&node).unwrap().prevs.is_empty() {
            return NodeResult::new();
        }
        let mut deps = Vec::new();
        for prev in nodes.get(&node).unwrap().prevs.iter() {
            let prev_ptr = Arc::new(prev);
            let dag_futures_ptr = Arc::clone(&dag_futures);
            let have_handled_ptr = Arc::clone(&have_handled);
            let nodes_ptr = Arc::clone(&nodes);

            if !have_handled.lock().unwrap().contains(&prev.to_string()) {
                dag_futures.lock().unwrap().insert(
                    prev.to_string(),
                    dfs_node(
                        dag_futures_ptr,
                        have_handled_ptr,
                        nodes_ptr,
                        prev_ptr.to_string(),
                    )
                    .boxed()
                    .shared(),
                );
                have_handled.lock().unwrap().insert(prev_ptr.to_string());
            }
            deps.push(dag_futures.lock().unwrap().get(prev).unwrap().clone());
        }

        join_all(deps)
            .then(|x| async move { NodeResult::new() })
            .await
        // return A::default();
    }
}

fn demo() {
    let _dag = Flow::<i32, i32>::new().register("handle_wrapper", Arc::new(handle_wrapper));
}

#[derive(Default, Clone, Debug)]
struct A {}

unsafe impl Send for A {}
unsafe impl Sync for A {}

#[async_recursion]
async fn dfs_node<'a>(
    mut dag_futures: Arc<
        Mutex<
            HashMap<
                std::string::String,
                Shared<Pin<Box<dyn futures::Future<Output = NodeResult> + std::marker::Send>>>,
            >,
        >,
    >,
    mut have_handled: Arc<Mutex<HashSet<String>>>,
    nodes: Arc<HashMap<String, Box<DAGNode>>>,
    node: String,
) -> NodeResult {
    println!("xxx {:?}", node);
    if nodes.get(&node).unwrap().prevs.is_empty() {
        return NodeResult::new();
    }
    let mut deps = Vec::new();
    for prev in nodes.get(&node).unwrap().prevs.iter() {
        let prev_ptr = Arc::new(prev);
        let dag_futures_ptr = Arc::clone(&dag_futures);
        let have_handled_ptr = Arc::clone(&have_handled);
        let nodes_ptr = Arc::clone(&nodes);

        if !have_handled.lock().unwrap().contains(&prev.to_string()) {
            dag_futures.lock().unwrap().insert(
                prev.to_string(),
                dfs_node(
                    dag_futures_ptr,
                    have_handled_ptr,
                    nodes_ptr,
                    prev_ptr.to_string(),
                )
                .boxed()
                .shared(),
            );
            have_handled.lock().unwrap().insert(prev_ptr.to_string());
        }
        deps.push(dag_futures.lock().unwrap().get(prev).unwrap().clone());
    }

    join_all(deps)
        .then(|x| async move { NodeResult::new() })
        .await
    // return A::default();
}
