use linkml_runtime_python::runtime_module;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[test]
fn python_reprs() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let modules = sys.getattr("modules").unwrap();
        let sys_modules = modules.cast::<PyDict>().unwrap();
        sys_modules.set_item("linkml_runtime", module).unwrap();

        let locals = PyDict::new(py);
        locals
            .set_item(
                "schema_yaml",
                r#"id: test
name: test
prefixes:
  test: https://example.org/
default_prefix: test
classes:
  Person:
    slots:
      - name
slots:
  name:
    range: string
"#,
            )
            .unwrap();
        pyo3::py_run!(
            py,
            *locals,
            r#"
import linkml_runtime as lr
sv = lr.make_schema_view()
sv.add_schema_str(schema_yaml)
cv = sv.get_class_view('Person')
svw = sv.get_slot_view('name')
assert repr(sv) == 'SchemaView(n_schemas=1, n_classes=1, n_slots=1)'
assert repr(cv) == "ClassView(name='Person', slots=1)"
assert repr(svw) == "SlotView(name='name', range='string')"
"#
        );
    });
}
