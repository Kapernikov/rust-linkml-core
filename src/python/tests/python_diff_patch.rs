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
fn diff_and_patch_via_python() {
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
                "current_path",
                data_path("person_valid.yaml").to_str().unwrap(),
            )
            .unwrap();
        locals
            .set_item(
                "personinfo_schema_path",
                data_path("personinfo.yaml").to_str().unwrap(),
            )
            .unwrap();
        locals
            .set_item(
                "container_valid_path",
                data_path("example_personinfo_data.yaml").to_str().unwrap(),
            )
            .unwrap();
        locals
            .set_item(
                "container_invalid_path",
                data_path("container_person_bad_email.yaml")
                    .to_str()
                    .unwrap(),
            )
            .unwrap();

        pyo3::py_run!(
            py,
            *locals,
            r#"
import linkml_runtime as lr

def assert_no_errors(issues):
    assert all(issue.severity != 'error' for issue in issues), issues

sv = lr.make_schema_view(schema_path)
cls = sv.get_class_view('Person')
older_json = '{"name": "Alicia", "age": 40, "internal_id": "id1"}'
older, older_issues = lr.load_json(older_json, sv, cls)
current, current_issues = lr.load_yaml(current_path, sv, cls)
assert older is not None
assert current is not None
assert older.schema_view is sv
assert current.schema_view is sv
assert_no_errors(older_issues)
assert_no_errors(current_issues)
deltas = lr.diff(older, current)
assert isinstance(deltas, list)
for d in deltas:
    assert isinstance(d, lr.Delta)
    assert d.op in {'add', 'remove', 'update'}
paths = {tuple(d.path) for d in deltas}
if paths != {('age',), ('name',)}:
    raise RuntimeError(('paths', paths, [(tuple(d.path), d.old, d.new) for d in deltas]))
age_delta = next(d for d in deltas if tuple(d.path) == ('age',))
if not (age_delta.old == 40 and age_delta.new == 33):
    raise RuntimeError(('age', age_delta.old, age_delta.new))
name_delta = next(d for d in deltas if tuple(d.path) == ('name',))
if not (name_delta.old == 'Alicia' and name_delta.new == 'Alice'):
    raise RuntimeError(('name', name_delta.old, name_delta.new))
result = lr.patch(older, deltas)
assert result.value['age'].as_python() == 33
assert result.value['internal_id'].as_python() == 'id1'
assert result.value['name'].as_python() == 'Alice'
assert result.trace.failed == []

# roundtrip through Python-side serialization and constructor
serialized = [d.to_dict() for d in deltas]
rebuilt = []
for item in serialized:
    rebuilt.append(
        lr.Delta(item['path'], item['op'], old=item['old'], new=item['new'])
    )
result2 = lr.patch(older, rebuilt)
assert result2.value['age'].as_python() == 33
assert result2.value['internal_id'].as_python() == 'id1'
assert result2.value['name'].as_python() == 'Alice'
assert result2.trace.failed == []

# failed delta is reported
bad_delta = lr.Delta(['bogus'], 'remove', old='x')
bad_result = lr.patch(older, [bad_delta])
assert bad_result.trace.failed == [['bogus']]

# Containers under personinfo schema still deserialize when validation errors exist.
sv_info = lr.make_schema_view(personinfo_schema_path)
container_cls = sv_info.get_class_view('Container')
valid_container, valid_issues = lr.load_yaml(container_valid_path, sv_info, container_cls)
invalid_container, invalid_issues = lr.load_yaml(container_invalid_path, sv_info, container_cls)
assert valid_container is not None
assert invalid_container is not None
assert_no_errors(valid_issues)
assert any(issue.severity == 'error' for issue in invalid_issues), invalid_issues

deltas_invalid = lr.diff(valid_container, invalid_container)
assert deltas_invalid
patched_invalid = lr.patch(valid_container, deltas_invalid)
assert patched_invalid.trace.failed == []
assert patched_invalid.value.as_python() == invalid_container.as_python()

deltas_valid = lr.diff(invalid_container, valid_container)
assert deltas_valid
patched_valid = lr.patch(invalid_container, deltas_valid)
assert patched_valid.trace.failed == []
assert patched_valid.value.as_python() == valid_container.as_python()
"#
        );
    });
}
