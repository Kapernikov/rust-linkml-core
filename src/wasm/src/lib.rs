use wasm_bindgen::prelude::*;

/// Simple greeting function to test wasm build
#[wasm_bindgen]
pub fn greet() -> String {
    "Hello from linkml wasm".to_string()
}
