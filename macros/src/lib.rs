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
    Result, Token, Pat,
};
use syn::FnArg;
use proc_macro2::{Span};
use std::any::Any;

fn get_val(val: &FnArg) -> Ident {
    let temp = if let FnArg::Typed(val) = val {
        match &*val.pat {
            Pat::Ident(i) => Some(i.ident.clone()),
            _ =>  None
        }
    } else {
        None
    };
    temp.unwrap()
}

#[proc_macro_attribute]
pub fn AnyFlowNode(params: TokenStream, code: TokenStream) -> TokenStream {
    let pp = code.clone();
    let qq = code.clone();
    let input = parse_macro_input!(pp as ItemFn);
    let input2 = parse_macro_input!(qq as ItemFn);
    let input_args = input2.sig.inputs.iter().cloned().collect::<Vec<FnArg>>();
    println!("input2 {:?}",input_args[0]);
    println!("xxxx {:?}", input.clone().sig.ident);
    println!(
        "block {:?}",
        input.clone().block.into_token_stream().to_string()
    );
    let fn_name = input.sig.ident;
    let fn_itself = input.block;
    let method_name = stringify!(fn_name);
    let params_info = parse_macro_input!(params as AttributeArgs);
    println!("xxxpppp {:?}", input_args.len());
    if (input_args.len() >3) || (input_args.len() < 2) {
        panic!("invalid input");
    }

    let first_arg = get_val(&input_args[0]);
    let second_arg = get_val(&input_args[1]);

    let mut fn_content = quote! {
        let handle = |#first_arg, #second_arg: Arc<anyflow::NodeResults>| {
            #fn_itself
        };
        handle(graph_args, input)
    };
    let mut fn_content2 = fn_content.clone();

    let mut config_type = Ident::new("Placeholder", Span::call_site());

    if params_info.len() > 0 && input_args.len() == 3{
        println!("params_info {:?}", params_info[0].clone());
        if let Meta(Path(ast)) = &params_info[0] {
            config_type = ast.segments[0].ident.clone();
        }
        println!("xxxx {:?}", config_type);
        let third_arg = &get_val(&input_args[2]);
        
        fn_content = quote! {
            let handle = |#first_arg, #second_arg: #config_type, #third_arg: Arc<anyflow::NodeResults>| {
                #fn_itself
            };
            let p: #config_type = serde_json::from_str(params.get()).unwrap();
            handle(graph_args, p, input)
        };

        fn_content2 =  quote! {
            let handle = |#first_arg, #second_arg: &#config_type, #third_arg: Arc<anyflow::NodeResults>| {
                #fn_itself
            };
            let p = params.downcast_ref::<#config_type>().unwrap();
            handle(graph_args, p, input)
        };
    }

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
        impl AnyHandler for #fn_name {
            fn config_generate<'a>(input: &'a Box<RawValue>) -> Box<(dyn Any + std::marker::Send)> {
                let c : Box<#config_type> = Box::new(serde_json::from_str(input.get()).unwrap());
                c
            }
            async fn async_calc<E: Send + Sync>(
                graph_args: Arc<E>,
                params: Box<RawValue>,
                input: Arc<anyflow::NodeResults>,
            ) -> NodeResult {
                #fn_content
            }

            async fn async_calc2<E: Send + Sync>(
                graph_args: Arc<E>,
                params: Box<Any + Send>,
                input: Arc<anyflow::NodeResults>,
            ) -> NodeResult {
                #fn_content2
            }
        }
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