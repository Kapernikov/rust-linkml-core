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
fn navigate_value_via_python() {
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
            .set_item(
                "schema_path",
                data_path("personinfo.yaml").to_str().unwrap(),
            )
            .unwrap();
        locals
            .set_item(
                "data_path",
                data_path("example_personinfo_data.yaml").to_str().unwrap(),
            )
            .unwrap();

        pyo3::py_run!(
            py,
            *locals,
            r#"
import linkml_runtime as lr
def load_no_errors(runtime, loader, path, sv, cls):
    value, issues = loader(path, sv, cls)
    assert value is not None
    assert all(issue.severity != 'error' for issue in issues), issues
    return value

sv = lr.make_schema_view(schema_path)
cls = sv.get_class_view('Container')
value = load_no_errors(lr, lr.load_yaml, data_path, sv, cls)

# Navigate top-level map key
assert 'objects' in value.keys()
assert value.navigate(['objects']) is not None

# Navigate list index then nested keys to scalar
name = value.navigate(['objects','2','has_medical_history','0','diagnosis','name'])
assert name is not None
assert name.as_python() == 'headache'

# Non-existent path -> None
assert value.navigate(['objects','1000']) is None
"#
        );
    });
}
