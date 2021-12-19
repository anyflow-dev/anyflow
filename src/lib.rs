pub mod dag;
pub use dag::AnyHandler;
pub use dag::AsyncHandler;
pub use dag::EmptyPlaceHolder;
pub use dag::Flow;
pub use dag::HandlerInfo;
pub use dag::HandlerType;
pub use dag::OpResult;
pub use dag::OpResults;

#[macro_export]
macro_rules! resgiter_node{
    ( $($x:ident),* ) => {
        &|| {
            let mut data: Vec<(
                &'static str,
                fn(
                    Arc<_>,
                    Arc<_>,
                    Arc<_>,
                ) -> Pin<Box<dyn futures::Future<Output = OpResult> + std::marker::Send>>,
                Arc<Fn(Box<serde_json::value::RawValue>) -> Arc<(dyn Any + std::marker::Send + Sync)> + Send + Sync>,
                bool,
            )> = Vec::new();
            $(
                #[allow(unused_assignments)]
                {
                    data.push((
                        $x::generate_config().name,
                        $x::async_calc2,
                        Arc::new($x::config_generate),
                        $x::generate_config().has_config,
                    ));
                }
            )*
            data
        }

    };
}
