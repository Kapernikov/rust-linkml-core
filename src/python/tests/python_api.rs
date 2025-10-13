use linkml_runtime_python::runtime_module;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;

fn meta_path() -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        base.join("../schemaview/tests/data/meta.yaml"),
        base.join("../runtime/tests/data/meta.yaml"),
        base.join("tests/data/meta.yaml"),
    ];
    for c in candidates {
        if c.exists() {
            return c;
        }
    }
    panic!("meta.yaml not found in known locations relative to python crate");
}

#[test]
fn construct_via_python() {
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
            .set_item("meta_path", meta_path().to_str().unwrap())
            .unwrap();
        pyo3::py_run!(
            py,
            *locals,
            r#"
import linkml_runtime as lr
sv = lr.make_schema_view(meta_path)
unresolved = sv.get_unresolved_schemas()
assert "https://w3id.org/linkml/mappings" in unresolved
"#
        );
    });
}

#[test]
fn definitions_via_python() {
    pyo3::prepare_freethreaded_python();
    let yaml = std::fs::read_to_string(meta_path()).unwrap();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let modules = sys.getattr("modules").unwrap();
        let sys_modules = modules.downcast::<PyDict>().unwrap();
        sys_modules.set_item("linkml_runtime", module).unwrap();

        let locals = PyDict::new(py);
        locals.set_item("meta_yaml", &yaml).unwrap();
        pyo3::py_run!(
            py,
            *locals,
            r#"
import linkml_runtime as lr
sv = lr.make_schema_view()
sv.add_schema_str(meta_yaml)
print('schemas', sv.get_unresolved_schemas())
s = sv.get_schema('https://w3id.org/linkml/meta')
print('schema', s)
assert s is not None and s.name == 'meta'
c = sv.get_class_view('linkml:class_definition')
print('class', c)
assert c is not None and c.name == 'class_definition'
cv_uri = sv.get_class_view_by_uri('https://w3id.org/linkml/ClassDefinition')
assert cv_uri is not None and cv_uri.name == 'class_definition'
slot = sv.get_slot_view_by_uri('skos:definition')
assert slot is not None
assert slot.schema_id() == 'https://w3id.org/linkml/meta'
assert slot.canonical_uri() == 'http://www.w3.org/2004/02/skos/core#definition'
class_def = sv.get_class_view('linkml:class_definition')
slot_def = sv.get_class_view('linkml:slot_definition')
assert class_def is not None and slot_def is not None
ancestor = type(class_def).most_specific_common_ancestor([class_def, slot_def])
assert ancestor is not None and ancestor.name == 'definition'
ancestor_with_mixins = type(class_def).most_specific_common_ancestor(
    [class_def, slot_def],
    include_mixins=True,
)
assert ancestor_with_mixins is not None and ancestor_with_mixins.name == 'definition'
assert sv.is_same(sv)
snapshot_yaml = sv.to_snapshot_yaml()
sv_clone = type(sv).from_snapshot_yaml(snapshot_yaml)
assert not sv.is_same(sv_clone)
"#
        );
    });
}
