extern crate proc_macro;
use anyflow;
use proc_macro::TokenStream;
use proc_macro::*;
use quote::quote;
use quote::ToTokens;
use syn::parse::*;
use syn::punctuated::Punctuated;
use syn::Field;
use syn::ItemStruct;
use syn::Meta::Path;
use syn::NestedMeta;
use syn::NestedMeta::Meta;
use syn::{
    braced, parse_macro_input, token, Attribute, AttributeArgs, DeriveInput, Ident, Item, ItemFn,
    Result, Token,
};

#[proc_macro_attribute]
pub fn AnyFlowNode(params: TokenStream, code: TokenStream) -> TokenStream {
    let pp = code.clone();
    let qq = code.clone();
    let input = parse_macro_input!(pp as ItemFn);
    let input2 = parse_macro_input!(qq as ItemFn);
    println!("xxxx {:?}", input.clone().sig.ident);
    println!(
        "block {:?}",
        input.clone().block.into_token_stream().to_string()
    );
    let fn_name = input.sig.ident;
    let fn_itself = input.block;
    let method_name = stringify!(fn_name);
    let params_info = parse_macro_input!(params as AttributeArgs);
    // println!("params_info {:?}", params_info[0].clone());
    let mut config_type: Option<syn::Ident> = None;
    if let Meta(Path(ast)) = &params_info[0] {
        config_type = Some(ast.segments[0].ident.clone());
    }
    println!("xxxx {:?}", config_type);

    let tokens = quote! {
        struct #fn_name {}

        impl #fn_name {
            fn generate_config() -> anyflow::HandlerInfo{
                HandlerInfo{
                name: stringify!(#fn_name),
                method_type: anyflow::HandlerType::Async,
                has_config: false,
                }
            }
        }

        #[async_trait]
        impl AnyHandler<'_,#config_type> for #fn_name {
            fn config_generate<'a>(input: &'a Box<RawValue>) -> Box<#config_type> {
                Box::new(#config_type::default())
            }
            async fn async_calc<E: Send + Sync>(
                graph_args: Arc<E>,
                params: Box<RawValue>,
                input: Arc<anyflow::NodeResults>,
            ) -> NodeResult {
                let handle = |graph_args, params: Box<RawValue>, input: Arc<anyflow::NodeResults>| {
                    #fn_itself
                };
                handle(graph_args, params, input)
            }
        }

        // struct SerializeWith #generics #where_clause {
        //     value: &'a #field_ty,
        //     phantom: core::marker::PhantomData<#item_ty>,
        // }

        // impl #generics serde::Serialize for SerializeWith #generics #where_clause {
        //     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        //     where
        //         S: serde::Serializer,
        //     {
        //         #path(self.value, serializer)
        //     }
        // }

        // SerializeWith {
        //     value: #value,
        //     phantom: core::marker::PhantomData::<#item_ty>,
        // }
    };
    println!("{}", format!("xxx {:?}", tokens.to_string()));
    tokens.into()
}

const SIZE: usize = 3;

struct MyVec {
    data: [i32; SIZE],
}

// fn demo() {
//     resgiter_node![1, 2];
// }
