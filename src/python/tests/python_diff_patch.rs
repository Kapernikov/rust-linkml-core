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
deltas = lr.diff(older, current, treat_missing_as_null=False)
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

deltas_invalid = lr.diff(valid_container, invalid_container, treat_missing_as_null=False)
assert deltas_invalid
patched_invalid = lr.patch(valid_container, deltas_invalid)
assert patched_invalid.trace.failed == []
assert patched_invalid.value.as_python() == invalid_container.as_python()

deltas_valid = lr.diff(invalid_container, valid_container, treat_missing_as_null=False)
assert deltas_valid
patched_valid = lr.patch(invalid_container, deltas_valid)
assert patched_valid.trace.failed == []
assert patched_valid.value.as_python() == valid_container.as_python()
"#
        );
    });
}

/// The identity label a single element answers with, and the segments the
/// whole list answers with, are two different questions — and a consumer that
/// renders an inlined list as an editable table needs both.
///
/// `list_path_segments` is all-or-nothing by design: the moment one element
/// carries no label the entire list is addressed positionally, so a table that
/// asked it per row would drop every row's provenance as soon as a user added
/// a row with the identity slot still empty. `element_identity_label` is the
/// per-element rule, which never consults the siblings.
///
/// Both must agree with what `diff` emits for the same data, or a path one
/// side records is a path the other cannot resolve.
#[test]
fn identity_labels_and_list_segments_via_python() {
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
                "identity_schema",
                r#"id: https://example.org/identity_labels
name: identity_labels
prefixes:
  ex: https://example.org/
default_prefix: ex
default_range: string
classes:
  Sheet:
    attributes:
      # single-slot unique_keys: the label is the bare value
      rows:
        range: Row
        multivalued: true
        inlined_as_list: true
      # composite unique_keys: the label is a JSON array
      readings:
        range: Reading
        multivalued: true
        inlined_as_list: true
      # no identity declared at all
      vertices:
        range: Vertex
        multivalued: true
        inlined_as_list: true
  Row:
    unique_keys:
      by_code:
        unique_key_slots: [code]
    attributes:
      # deliberately NOT required: a freshly added row may leave it empty
      code: {range: string}
      note: {range: string}
  Reading:
    unique_keys:
      by_station_and_kind:
        unique_key_slots: [station, primary]
    attributes:
      station: {range: string}
      primary: {range: boolean}
      value: {range: string}
  Vertex:
    attributes:
      x: {range: float}
      y: {range: float}
"#,
            )
            .unwrap();

        pyo3::py_run!(
            py,
            *locals,
            r#"
import json
import linkml_runtime as lr

sv = lr.make_schema_view()
sv.add_schema_str(identity_schema)
sheet = sv.get_class_view('Sheet')

# `py_run!` gives the script a locals dict, so a function body — which looks
# names up in globals — cannot see `lr`, `sv` or `sheet`. Bind them as
# defaults, the way `python_navigate` passes the module in as an argument.
def load(payload, lr=lr, json=json, sv=sv, sheet=sheet):
    value, issues = lr.load_json(json.dumps(payload), sv, sheet)
    assert value is not None
    assert all(issue.severity != 'error' for issue in issues), issues
    return value

def row(code, note):
    return {'note': note} if code is None else {'code': code, 'note': note}

def reading(primary, value):
    return {'station': 'A', 'primary': primary, 'value': value}

full = load({
    'rows': [row('R1', 'one'), row('R2', 'two')],
    'readings': [reading(True, '1.0'), reading(False, '2.0')],
    'vertices': [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}],
})

def labels(node):
    return [element.element_identity_label() for element in node.values()]

# Single-slot unique_keys: the bare value, per element and for the list.
rows = full.navigate(['rows'])
assert labels(rows) == ['R1', 'R2'], labels(rows)
assert rows.list_path_segments() == ['R1', 'R2'], rows.list_path_segments()

# Composite unique_keys: a compact JSON array in `unique_key_slots` order,
# booleans spelled the JSON way (`true`), not the Python way (`True`).
readings = full.navigate(['readings'])
assert labels(readings) == ['["A","true"]', '["A","false"]'], labels(readings)
assert readings.list_path_segments() == ['["A","true"]', '["A","false"]']

# No identity declared: no label, and the list is addressed by position.
vertices = full.navigate(['vertices'])
assert labels(vertices) == [None, None], labels(vertices)
assert vertices.list_path_segments() == ['0', '1'], vertices.list_path_segments()

# `list_path_segments` is a question only a list can answer.
assert full.list_path_segments() is None
assert full.navigate(['rows', 'R1']).list_path_segments() is None
assert full.navigate(['rows', 'R1', 'note']).list_path_segments() is None
# ...and an object with no identity has no label either.
assert full.element_identity_label() is None

# Those labels are exactly the segments `diff` emits for the same data.
edited = load({
    'rows': [row('R1', 'one'), row('R2', 'TWO')],
    'readings': [reading(True, '1.0'), reading(False, '9.9')],
    'vertices': [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}],
})
paths = {tuple(d.path) for d in lr.diff(full, edited, treat_missing_as_null=False)}
assert paths == {
    ('rows', 'R2', 'note'),
    ('readings', '["A","false"]', 'value'),
}, paths

# The case the per-element call exists for: a user adds a row and has not
# filled its identity slot in yet. The whole table flips to positional
# addressing — `diff` included, so the segments stay resolvable...
partial = load({'rows': [row('R1', 'one'), row('R2', 'two'), row(None, 'fresh')]})
partial_rows = partial.navigate(['rows'])
assert partial_rows.list_path_segments() == ['0', '1', '2'], partial_rows.list_path_segments()

# ...while every labelled element still answers with its own label, which is
# what lets a row keep its provenance across the neighbour's empty slot.
assert labels(partial_rows) == ['R1', 'R2', None], labels(partial_rows)

partial_edited = load({'rows': [row('R1', 'one'), row('R2', 'TWO'), row(None, 'fresh')]})
partial_paths = {
    tuple(d.path)
    for d in lr.diff(partial, partial_edited, treat_missing_as_null=False)
}
assert partial_paths == {('rows', '1', 'note')}, partial_paths
"#
        );
    });
}
