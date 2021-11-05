extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro::*;
use quote::quote;
use syn::{parse_macro_input, Attribute, AttributeArgs, DeriveInput, Ident, Result, Item, Token, braced, token, ItemFn};
use syn::punctuated::Punctuated;
use syn::parse::*;
use syn::ItemStruct;
use syn::Field;
use quote::ToTokens;


struct MyMacroInput {
    fn_token: Token![fn],
    ident: Ident,
    // brace_token: token::Paren,
    // fields: Punctuated<Field, Token![,]>,
}

impl Parse for MyMacroInput {
    fn parse(input: ParseStream) -> Result<Self> {
        // let content;
        Ok(MyMacroInput {
            fn_token: input.parse()?,
            ident: input.parse()?,
            // brace_token: input.parse()?,
            // fields: content.parse_terminated(Field::parse_named)?,
        })
    }
}

#[proc_macro_attribute]
pub fn AnyFlowNode(params: TokenStream, code: TokenStream) -> TokenStream {
    let pp = code.clone();
    let qq = code.clone();
    let input = parse_macro_input!(pp as ItemFn);
    let input2 = parse_macro_input!(qq as ItemFn);
    println!("xxxx {:?}", input.clone().sig.ident);
    println!("block {:?}", input.clone().block.into_token_stream().to_string());
    let fn_name = input.sig.ident;
    let fn_itself = input.block;
    let method_name = stringify!(fn_name);
    let params_info = parse_macro_input!(params as AttributeArgs);
    println!("params_info {:?}", params_info.clone());

    let tokens = quote! {
        // #fn_itself


        struct #fn_name {}

        impl #fn_name {
            fn generate_config() -> HandlerInfo{
                HandlerInfo{
                name: "xxx",
                method_type: HandlerType::Async,
                has_config: false,
                }
            }
        }

        impl AnyHandler<'_,X> for #fn_name {
            fn config_generate<'a>(input: &'a Box<RawValue>) -> Box<X> {
                Box::new(X::default())
            }
            fn async_calc<E: Send + Sync>(
                graph_args: Arc<E>,
                params: Box<RawValue>,
                input: Arc<NodeResults>,
            ) -> NodeResult {
                let handle = |graph_args, params: Box<RawValue>, input: Arc<NodeResults>| {
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

macro_rules! resgiter{
    // first arm in case of single argument and last remaining variable/number
       ($a:expr)=>{
           $a
       };
   // second arm in case of two arument are passed and stop recursion in case of odd number ofarguments
       ($a:expr,$b:expr)=>{
           {
               $a+$b
           }
       };
   // add the number and the result of remaining arguments
       ($a:expr,$($b:tt)*)=>{
          {
              $a+add!($($b)*)
          }
       }
   }

fn demo() {
    resgiter!(1, 2);
}