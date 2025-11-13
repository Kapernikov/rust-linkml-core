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
fn python_equals_api() {
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

        pyo3::py_run!(
            py,
            *locals,
            r#"
import linkml_runtime as lr
import json

def load_no_errors(runtime, payload, sv, cls):
    value, issues = runtime.load_json(payload, sv, cls)
    assert value is not None
    assert all(issue.severity != 'error' for issue in issues), issues
    return value

sv = lr.make_schema_view(schema_path)
cls = sv.get_class_view('Container')

doc1 = {
  'objects': [
    {
      'objecttype': 'personinfo:Person',
      'id': 'P:1',
      'name': 'Alice',
      'current_address': None
    }
  ]
}
doc2 = {
  'objects': [
    {
      'objecttype': 'personinfo:Person',
      'id': 'P:1',
      'name': 'Alice'
    }
  ]
}

v1 = load_no_errors(lr, json.dumps(doc1), sv, cls)
v2 = load_no_errors(lr, json.dumps(doc2), sv, cls)
assert v1['objects'][0].equals(v2['objects'][0], True)
"#
        );
    });
}
