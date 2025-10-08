#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::todo,
        clippy::unwrap_used
    )
)]

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

/// Greet the provided name.
///
/// This simple function is exported to WebAssembly for testing purposes and
/// avoids any filesystem access so it works in typical browser environments.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn greet(name: &str) -> String {
    format!("Hello, {name}!")
}
