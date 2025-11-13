use linkml_runtime_python::runtime_module;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;

fn data_path(name: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        base.join("../runtime/tests/data").join(name),
        base.join("../schemaview/tests/data").join(name),
        base.join("tests/data").join(name),
    ];
    for c in candidates {
        if c.exists() {
            return c;
        }
    }
    panic!("test data not found: {}", name);
}

#[test]
fn load_value_via_python() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let modules = sys.getattr("modules").unwrap();
        let sys_modules = modules.downcast::<PyDict>().unwrap();
        sys_modules.set_item("linkml_runtime", module).unwrap();

        let locals = PyDict::new(py);
        locals
            .set_item("schema_path", data_path("schema.yaml").to_str().unwrap())
            .unwrap();
        locals
            .set_item(
                "person_path",
                data_path("person_valid.yaml").to_str().unwrap(),
            )
            .unwrap();

        pyo3::py_run!(
            py,
            *locals,
            r#"
import linkml_runtime as lr
def load_no_errors(loader, path, sv, cls):
    value, issues = loader(path, sv, cls)
    assert value is not None
    assert all(issue.severity != 'error' for issue in issues), issues
    return value

sv = lr.make_schema_view(schema_path)
cls = sv.get_class_view('Person')
value = load_no_errors(lr.load_yaml, person_path, sv, cls)
assert value.class_name == 'Person'
assert value.class_definition.name == 'Person'
assert value['name'].slot_name == 'name'
assert value['name'].slot_definition.name == 'name'
assert value['name'].class_definition.name == 'Person'
assert value['name'].as_python() == 'Alice'
"#
        );
    });
}
