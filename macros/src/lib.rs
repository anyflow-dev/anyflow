extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro::*;
use quote::quote;
use syn::{parse_macro_input, Attribute, DeriveInput, Ident, Result, Item, Token, braced, token};
use syn::punctuated::Punctuated;
use syn::parse::*;
use syn::ItemStruct;
use syn::Field;


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
    let input = parse_macro_input!(pp as MyMacroInput);
    // let struct_name = &input.ident.to_string();
    // println!("xxohiohhoihiox: {:?}", struct_name);

//     let x = format!(
//         r#"
//     {code}
//     #[async_trait]
//     impl AsyncNode for ANode {{
//         // type Params = {params};
//     // fn deserialize(self, params_ptr: &Box<RawValue>) -> AnyParams {{
//     //     serde_json::from_str(params_ptr.get()).unwrap()
//     // }}

//     async fn handle<'a, E: Send + Sync>(
//         self,
//         graph_args: &'a Arc<E>,
//         input: Arc<NodeResult>,
//         // params: Arc<AnyParams>,
//     ) -> NodeResult {{

//         return self.handle_wrapper(graph_args, input).await;
//     }}

//     fn name() -> &'static str {{
//         return "{struct_name}"
//     }}
// }}
// "#,
//         params = params.to_string(),
//         code = code.to_string(),
//         struct_name = struct_name,
//     );

let x = format!(r#"
fn dummy() {{
}}
"#,
);

    x.parse().expect("Generated invalid tokens")
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