//! Every name the generated stub attributes to the extension module must be
//! importable from it.
//!
//! `pyo3-stub-gen` files a `#[gen_stub_pyclass]` under the extension module by
//! default, so `python/linkml_runtime_rust/_native.pyi` declares
//! `SchemaView`, `ClassView`, `Annotation`, `Extension`, every metamodel struct
//! — and names them throughout its own signatures. Registering them anywhere
//! else, or nowhere, makes the stub describe an API that does not exist:
//! `from linkml_runtime_rust._native import Annotation` type-checks cleanly and
//! raises `ImportError`. That is worse than a missing stub, because a checker
//! blesses the broken import.
//!
//! This asserts the module's real contents against the names the stub declares,
//! so a new `#[gen_stub_pyclass]` cannot be added without being registered.

use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::path::PathBuf;

fn stub_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/linkml_runtime_rust/_native.pyi")
}

/// The top-level `class <Name>:` declarations in the generated stub.
fn classes_declared_in_the_stub(stub: &str) -> Vec<&str> {
    stub.lines()
        .filter_map(|line| line.strip_prefix("class "))
        .filter_map(|rest| rest.split([':', '(']).next())
        .filter(|name| !name.is_empty())
        .collect()
}

#[test]
fn every_class_the_stub_declares_is_importable_from_the_module() {
    let stub = std::fs::read_to_string(stub_path()).expect("generated stub is missing");
    let declared = classes_declared_in_the_stub(&stub);
    assert!(
        declared.len() > 40,
        "only found {} classes in the stub; the parse is wrong, not the module",
        declared.len()
    );

    pyo3::prepare_freethreaded_python();
    let missing = Python::with_gil(|py| {
        let module = PyModule::new(py, "linkml_runtime_rust_native_under_test").unwrap();
        linkml_runtime_python::runtime_module(&module).unwrap();
        declared
            .iter()
            .filter(|name| !module.as_any().hasattr(**name).unwrap())
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>()
    });

    assert!(
        missing.is_empty(),
        "{} of {} classes declared in _native.pyi are not registered in the \
         module, so `from linkml_runtime_rust._native import <name>` type-checks \
         and then raises ImportError. Register them in `runtime_module`:\n  {}",
        missing.len(),
        declared.len(),
        missing.join("\n  "),
    );
}
