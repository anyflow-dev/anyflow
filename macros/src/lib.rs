extern crate proc_macro;
use anyflow;
use proc_macro::TokenStream;
use proc_macro::*;
use proc_macro2::Span;
use quote::quote;
use quote::ToTokens;
use std::any::Any;
use syn::parse::*;
use syn::punctuated::Punctuated;
use syn::Field;
use syn::FnArg;
use syn::ItemStruct;
use syn::Meta::Path;
use syn::NestedMeta;
use syn::NestedMeta::Meta;
use syn::{
    braced, parse_macro_input, token, Attribute, AttributeArgs, DeriveInput, Ident, Item, ItemFn,
    Pat, PathSegment, Result, Token, Type,
};

fn get_val(val: &FnArg) -> Ident {
    let temp = if let FnArg::Typed(val) = val {
        match &*val.pat {
            Pat::Ident(i) => Some(i.ident.clone()),
            _ => None,
        }
    } else {
        None
    };
    temp.unwrap()
}

fn get_type(val: &FnArg) -> PathSegment {
    let temp = if let FnArg::Typed(val) = val {
        match &*val.ty {
            Type::Path(i) => {
                println!("xxx {:?}", i.path.segments[0]);
                Some(i.path.segments[0].clone())
            }
            _ => None,
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
    println!("input2 {:?}", input_args[0]);
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
    if (input_args.len() > 3) || (input_args.len() < 2) {
        panic!("invalid input");
    }

    let first_arg = get_val(&input_args[0]);
    let second_arg = get_val(&input_args[1]);
    let first_arg_type = get_type(&input_args[0]);

    let mut fn_content = quote! {
        let handle = |#first_arg, #second_arg: Arc<anyflow::OpResults>| {
            #fn_itself
        };
        handle(graph_args, input)
    };
    let mut fn_content2 = fn_content.clone();

    let mut config_type : syn::Ident = Ident::new("EmptyPlaceHolder", Span::call_site());

    if params_info.len() > 0 && input_args.len() == 3 {
        println!("params_info {:?}", params_info[0].clone());
        println!("params_info {:?}", input_args.clone());
        if let Meta(Path(ast)) = &params_info[0] {
            config_type = ast.segments[0].ident.clone();
        }
        println!("xxxx {:?}", config_type);
        let third_arg = &get_val(&input_args[2]);

        fn_content = quote! {
            let handle = |#first_arg: #first_arg_type, #second_arg: #config_type, #third_arg: Arc<anyflow::OpResults>| {
                #fn_itself
            };
            let p: #config_type = serde_json::from_str(params.get()).unwrap();
            handle(graph_args, p, input)
        };

        fn_content2 = quote! {
            let handle = |#first_arg: #first_arg_type, #second_arg: &#config_type, #third_arg: Arc<anyflow::OpResults>| {
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
            type Req = #first_arg_type;
            fn config_generate(input: Box<RawValue>)
                -> Arc<(dyn Any + std::marker::Send + Sync)> {
                let c : Arc<#config_type> = Arc::new(serde_json::from_str(input.get()).unwrap());
                c
            }

            async fn async_calc2(
                graph_args: #first_arg_type,
                params: Arc<Any + Send + Sync>,
                input: Arc<anyflow::OpResults>,
            ) -> OpResult {
                #fn_content2
            }
        }
    };
    println!("{}", format!("xxx {:?}", tokens.to_string()));
    tokens.into()
}

#[proc_macro_attribute]
pub fn SimpleNode(params: TokenStream, code: TokenStream) -> TokenStream {
    let pp = code.clone();
    let qq = code.clone();
    let input = parse_macro_input!(pp as ItemFn);
    let input2 = parse_macro_input!(qq as ItemFn);
    let input_args = input2.sig.inputs.iter().cloned().collect::<Vec<FnArg>>();
    println!("input2 {:?}", input_args[0]);
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
    if input_args.len() < 1 {
        panic!("invalid input");
    }
    let first_arg_type = get_type(&input_args[0]);
    let first_arg = get_val(&input_args[0]);
    let mut config_type = quote! {anyflow::EmptyPlaceHolder}; // make a default one

    let mut extract_args = vec![quote! {}];
    for i in 1..input_args.len() {
        let arg_name = get_val(&input_args[i]);
        let arg_type = get_type(&input_args[i]);
        let idx = i - 1;
        extract_args.push(quote! {
            let #arg_name = input.get::<#arg_type>( #idx );
        })
    }

    let mut fn_content = quote! {
        let handle = |#first_arg| {
            #fn_itself
        };
        handle(graph_args, input)
    };

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
            type Req = #first_arg_type;
            fn config_generate(input: Box<RawValue>)
                -> Arc<(dyn Any + std::marker::Send + Sync)> {
                let c : Arc<#config_type> = Arc::new(serde_json::from_str(input.get()).unwrap());
                c
            }

            async fn async_calc2(
                graph_args: #first_arg_type,
                params: Arc<Any + Send + Sync>,
                input: Arc<anyflow::OpResults>,
            ) -> OpResult {
                OpResult::default()
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
