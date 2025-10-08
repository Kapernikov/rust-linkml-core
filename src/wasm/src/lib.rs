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

#[cfg(target_arch = "wasm32")]
use linkml_schemaview::schemaview::SchemaView;

/// Greet the provided name.
///
/// This simple function is exported to WebAssembly for testing purposes and
/// avoids any filesystem access so it works in typical browser environments.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn greet(name: &str) -> String {
    format!("Hello, {name}!")
}

/// Lightweight wrapper exposing `SchemaView` snapshot helpers to JavaScript.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub struct JsSchemaView {
    inner: SchemaView,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl JsSchemaView {
    #[wasm_bindgen(constructor)]
    pub fn new() -> JsSchemaView {
        JsSchemaView {
            inner: SchemaView::new(),
        }
    }

    /// Replace the current view with the one encoded in the snapshot YAML.
    #[wasm_bindgen(js_name = "loadSnapshotYaml")]
    pub fn load_snapshot_yaml(&mut self, yaml: &str) -> Result<(), JsValue> {
        let view = SchemaView::from_snapshot_yaml(yaml)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        self.inner = view;
        Ok(())
    }

    /// Return the view encoded as snapshot YAML.
    #[wasm_bindgen(js_name = "toSnapshotYaml")]
    pub fn to_snapshot_yaml(&self) -> Result<String, JsValue> {
        self.inner
            .to_snapshot_yaml()
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }
}

/// Construct a new `JsSchemaView` directly from snapshot YAML.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn schemaview_from_snapshot_yaml(yaml: &str) -> Result<JsSchemaView, JsValue> {
    let mut view = JsSchemaView::new();
    view.load_snapshot_yaml(yaml)?;
    Ok(view)
}
