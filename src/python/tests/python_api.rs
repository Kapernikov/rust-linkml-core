use linkml_runtime_python::runtime_module;
use pyo3::prelude::{PyAnyMethods, *};
use pyo3::types::{PyAny, PyDict};
use std::ffi::CString;
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

fn simple_enum_path() -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        base.join("../schemaview/tests/data/simple_enum.yaml"),
        base.join("tests/data/simple_enum.yaml"),
    ];
    for c in candidates {
        if c.exists() {
            return c;
        }
    }
    panic!("simple_enum.yaml not found in known locations relative to python crate");
}

fn path_traversal_path() -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        base.join("../schemaview/tests/data/path_traversal.yaml"),
        base.join("tests/data/path_traversal.yaml"),
    ];
    for c in candidates {
        if c.exists() {
            return c;
        }
    }
    panic!("path_traversal.yaml not found in known locations relative to python crate");
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
        let script = CString::new(
            r#"
import linkml_runtime as lr
sv = lr.make_schema_view()
sv.add_schema_str(meta_yaml)
results = {}
results['unresolved'] = sv.get_unresolved_schemas()
schema = sv.get_schema('https://w3id.org/linkml/meta')
results['schema_name'] = None if schema is None else schema.name
class_view = sv.get_class_view('linkml:class_definition')
results['class_view_name'] = None if class_view is None else class_view.name
class_by_uri = sv.get_class_view_by_uri('https://w3id.org/linkml/ClassDefinition')
results['class_by_uri_name'] = None if class_by_uri is None else class_by_uri.name
slot = sv.get_slot_view_by_uri('skos:definition')
results['slot_schema_id'] = None if slot is None else slot.schema_id()
results['slot_canonical'] = None if slot is None else slot.canonical_uri()
class_def = sv.get_class_view('linkml:class_definition')
slot_def = sv.get_class_view('linkml:slot_definition')
ancestor = None
ancestor_with_mixins = None
if class_def is not None and slot_def is not None:
    ancestor_obj = type(class_def).most_specific_common_ancestor([class_def, slot_def])
    ancestor = None if ancestor_obj is None else ancestor_obj.name
    ancestor_mix_obj = type(class_def).most_specific_common_ancestor(
        [class_def, slot_def],
        include_mixins=True,
    )
    ancestor_with_mixins = None if ancestor_mix_obj is None else ancestor_mix_obj.name
results['ancestor'] = ancestor
results['ancestor_with_mixins'] = ancestor_with_mixins
results['is_same_self'] = sv.is_same(sv)
snapshot_yaml = sv.to_snapshot_yaml()
sv_clone = type(sv).from_snapshot_yaml(snapshot_yaml)
results['is_same_clone'] = sv.is_same(sv_clone)
"#,
        )
        .unwrap();
        if let Err(err) = py.run(script.as_c_str(), None, Some(&locals)) {
            let value = err.value(py);
            let message = match value.str() {
                Ok(s) => s.to_string_lossy().into_owned(),
                Err(_) => "<no message>".to_string(),
            };
            panic!("python script execution failed: {}", message);
        }

        let results_value = locals
            .get_item("results")
            .expect("results lookup failed")
            .expect("results missing");
        let results = results_value
            .downcast::<PyDict>()
            .expect("results not a dict");

        let schema_name_value: Bound<'_, PyAny> = results
            .get_item("schema_name")
            .expect("schema_name lookup failed")
            .expect("schema_name missing");
        let schema_name: Option<String> =
            schema_name_value.extract().expect("schema_name wrong type");
        assert_eq!(schema_name.as_deref(), Some("meta"));

        let class_view_name_value: Bound<'_, PyAny> = results
            .get_item("class_view_name")
            .expect("class_view_name lookup failed")
            .expect("class_view_name missing");
        let class_view_name: Option<String> = class_view_name_value
            .extract()
            .expect("class_view_name wrong type");
        assert_eq!(class_view_name.as_deref(), Some("class_definition"));

        let class_by_uri_value: Bound<'_, PyAny> = results
            .get_item("class_by_uri_name")
            .expect("class_by_uri lookup failed")
            .expect("class_by_uri_name missing");
        let class_by_uri_name: Option<String> = class_by_uri_value
            .extract()
            .expect("class_by_uri_name wrong type");
        assert_eq!(class_by_uri_name.as_deref(), Some("class_definition"));

        let slot_schema_value: Bound<'_, PyAny> = results
            .get_item("slot_schema_id")
            .expect("slot_schema lookup failed")
            .expect("slot_schema_id missing");
        let slot_schema_id: Option<String> = slot_schema_value
            .extract()
            .expect("slot_schema_id wrong type");
        assert_eq!(
            slot_schema_id.as_deref(),
            Some("https://w3id.org/linkml/meta")
        );

        let canonical_value: Bound<'_, PyAny> = results
            .get_item("slot_canonical")
            .expect("slot_canonical lookup failed")
            .expect("slot_canonical missing");
        let canonical: Option<String> = canonical_value
            .extract()
            .expect("slot_canonical wrong type");
        assert_eq!(
            canonical.as_deref(),
            Some("http://www.w3.org/2004/02/skos/core#definition")
        );

        let ancestor_value: Bound<'_, PyAny> = results
            .get_item("ancestor")
            .expect("ancestor lookup failed")
            .expect("ancestor missing");
        let ancestor: Option<String> = ancestor_value.extract().expect("ancestor wrong type");
        assert_eq!(ancestor.as_deref(), Some("definition"));

        let ancestor_mix_value: Bound<'_, PyAny> = results
            .get_item("ancestor_with_mixins")
            .expect("ancestor_with_mixins lookup failed")
            .expect("ancestor_with_mixins missing");
        let ancestor_with_mixins: Option<String> = ancestor_mix_value
            .extract()
            .expect("ancestor_with_mixins wrong type");
        assert_eq!(ancestor_with_mixins.as_deref(), Some("definition"));

        let is_same_self_value: Bound<'_, PyAny> = results
            .get_item("is_same_self")
            .expect("is_same_self lookup failed")
            .expect("is_same_self missing");
        let is_same_self: bool = is_same_self_value
            .extract()
            .expect("is_same_self wrong type");
        assert!(is_same_self);

        let is_same_clone_value: Bound<'_, PyAny> = results
            .get_item("is_same_clone")
            .expect("is_same_clone lookup failed")
            .expect("is_same_clone missing");
        let is_same_clone: bool = is_same_clone_value
            .extract()
            .expect("is_same_clone wrong type");
        assert!(!is_same_clone);
    });
}

#[test]
fn slots_for_path_exposed() {
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
                "path_schema",
                path_traversal_path().to_str().expect("unicode path"),
            )
            .unwrap();
        let script = CString::new(
            r#"
import linkml_runtime as lr
sv = lr.make_schema_view(path_schema)
slots = sv.slots_for_path(
    "Person",
    ["workplaces", "departments", "location", "address", "street"],
)
results = {
    "count": len(slots),
    "names": sorted(slot.name for slot in slots),
    "after_scalar": len(
        sv.slots_for_path("Person", ["favorite_number", "bogus"])
    ),
}
"#,
        )
        .unwrap();
        if let Err(err) = py.run(script.as_c_str(), None, Some(&locals)) {
            let value = err.value(py);
            let message = match value.str() {
                Ok(s) => s.to_string_lossy().into_owned(),
                Err(_) => "<no message>".to_string(),
            };
            panic!("python script execution failed: {}", message);
        }

        let results_value = locals
            .get_item("results")
            .expect("results lookup failed")
            .expect("results missing");
        let results = results_value
            .downcast::<PyDict>()
            .expect("results not a dict");

        let count: usize = results
            .get_item("count")
            .expect("count lookup failed")
            .expect("count missing")
            .extract()
            .expect("count wrong type");
        assert!(count >= 1, "expected at least one slot match");

        let names_value = results
            .get_item("names")
            .expect("names lookup failed")
            .expect("names missing");
        let names: Vec<String> = names_value.extract().expect("names wrong type");
        assert!(names.iter().any(|name| name == "street"));

        let after_scalar: usize = results
            .get_item("after_scalar")
            .expect("after_scalar lookup failed")
            .expect("after_scalar missing")
            .extract()
            .expect("after_scalar wrong type");
        assert_eq!(after_scalar, 0);
    });
}

#[test]
fn enum_canonical_via_python() {
    pyo3::prepare_freethreaded_python();
    let yaml = std::fs::read_to_string(simple_enum_path()).unwrap();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let modules = sys.getattr("modules").unwrap();
        let sys_modules = modules.downcast::<PyDict>().unwrap();
        sys_modules.set_item("linkml_runtime", module).unwrap();

        let locals = PyDict::new(py);
        locals.set_item("schema_yaml", &yaml).unwrap();
        let script = CString::new(
            r#"
import linkml_runtime as lr
sv = lr.make_schema_view()
sv.add_schema_str(schema_yaml)
ev = sv.get_enum_view('simple:SignalTypes')
assert ev is not None
assert ev.canonical_uri() == 'https://example.org/simple/SignalTypes'
"#,
        )
        .unwrap();
        if let Err(err) = py.run(script.as_c_str(), None, Some(&locals)) {
            let value = err.value(py);
            let message = match value.str() {
                Ok(s) => s.to_string_lossy().into_owned(),
                Err(_) => "<no message>".to_string(),
            };
            panic!("python script execution failed: {}", message);
        }
    });
}
