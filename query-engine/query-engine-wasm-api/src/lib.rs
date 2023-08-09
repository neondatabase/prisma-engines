pub mod engine;
pub mod error;
pub mod functions;

pub(crate) type Result<T> = std::result::Result<T, error::ApiError>;
pub(crate) type Executor = Box<dyn query_core::QueryExecutor + Send + Sync>;

use wasm_bindgen::prelude::wasm_bindgen;

extern crate wee_alloc;
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen(js_name = "initPanicHook")]
pub fn init_panic_hook() {
    use log::Level;
    console_log::init_with_level(Level::Trace).expect("error initializing log");
    console_error_panic_hook::set_once();
}
