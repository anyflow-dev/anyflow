pub mod dag;
// pub use dag::Flow;
pub use dag::AnyHandler;
pub use dag::AsyncHandler;
pub use dag::HandlerInfo;
pub use dag::HandlerType;
pub use dag::NodeResult;
pub use dag::NodeResults;

#[macro_export]
macro_rules! resgiter_node{
    ( $($x:ident),* ) => {
        // let c = $x;
        &|| {
            let mut data:Vec<(&'static str,
            fn(Arc<_>, Box<_>, Arc<_>) -> Pin<Box<dyn futures::Future<Output = NodeResult> + std::marker::Send>>)>
            =  Vec::new();
            $(
                #[allow(unused_assignments)]
                {
                    data.push((
                        $x::generate_config().name,
                        $x::async_calc
                    ));
                }
            )*
            data
        }

    };
}
