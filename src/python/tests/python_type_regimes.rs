//! Python surface of the RDF type regimes: `regime=` on the Turtle writers,
//! quad output per type partition, and the importer reading regime output back.
use linkml_runtime_python::runtime_module;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::ffi::CString;
use std::path::PathBuf;

fn data_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../runtime/tests/data")
        .join(name)
}

#[test]
fn type_regimes_via_python() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let sys_modules = sys.getattr("modules").unwrap();
        let sys_modules = sys_modules.downcast::<PyDict>().unwrap();
        sys_modules.set_item("linkml_runtime", module).unwrap();

        let locals = PyDict::new(py);
        for (key, file) in [
            ("schema_path", "type_regimes_schema.yaml"),
            ("types_path", "types.yaml"),
            ("data_path", "type_regimes_roundtrip_data.yaml"),
        ] {
            locals
                .set_item(key, data_path(file).to_str().unwrap())
                .unwrap();
        }

        // Globals == locals so the helper functions below see each other.
        let script = CString::new(
            r#"
import io
import linkml_runtime as lr

NS = "https://example.com/type-regimes/"
SESAME = "http://www.openrdf.org/schema/sesame#directType"

sv = lr.make_schema_view()
sv.add_schema_from_path(schema_path)
with open(types_path) as f:
    sv.add_schema_str_with_import_ref(f.read(), "https://example.com/type-regimes", "linkml:types")
value, issues = lr.load_yaml(data_path, sv, sv.get_class_view("Registry"))
assert value is not None and all(i.severity != "error" for i in issues), issues

def nt_lines(text):
    return {line for line in text.splitlines() if line.strip()}

def graphs_of(nquads):
    out = {}
    for line in nt_lines(nquads):
        assert line.endswith(" .")
        body, graph = line[:-2].rsplit(" ", 1)
        out.setdefault(graph.strip("<>"), set()).add(body)
    return out

# Regime as a set of partitions.
assert [p for p in lr.TypeRegime.IS_A_AND_MIXINS.partitions()] == [
    lr.TypePartition.Own, lr.TypePartition.IsA, lr.TypePartition.Mixin, lr.TypePartition.Direct,
]
assert lr.TypeRegime.ASSERTED.partitions() == [lr.TypePartition.Own]
assert lr.TypeRegime.IS_A.partitions() == [
    lr.TypePartition.Own, lr.TypePartition.IsA, lr.TypePartition.Direct,
]

P = lr.TypePartition
custom = lr.TypeRegime([P.IsA, P.Own])
assert custom.partitions() == [P.Own, P.IsA]
assert custom == lr.TypeRegime.parse("own+is_a") == lr.TypeRegime(iter([P.Own, P.IsA, P.Own]))
assert str(custom) == "own+is_a" and repr(custom) == "TypeRegime.parse('own+is_a')"
assert str(lr.TypeRegime.IS_A) == "is_a" and repr(lr.TypeRegime.IS_A) == "TypeRegime.IS_A"
assert repr(lr.TypeRegime.ASSERTED) == "TypeRegime.ASSERTED"
assert lr.TypeRegime.parse("is_a_and_mixins") == lr.TypeRegime.IS_A_AND_MIXINS
assert str(lr.TypeRegime([])) == "none"
assert lr.TypeRegime.ASSERTED | lr.TypeRegime([P.IsA, P.Direct]) == lr.TypeRegime.IS_A
assert P.Mixin in lr.TypeRegime.IS_A_AND_MIXINS and P.Mixin not in lr.TypeRegime.IS_A
assert lr.TypeRegime.IS_A != lr.TypeRegime.IS_A_AND_MIXINS
seen = {lr.TypeRegime.IS_A: "a", lr.TypeRegime([P.Own, P.IsA, P.Direct]): "b"}
assert seen == {lr.TypeRegime.IS_A: "b"}
for bad in ("bogus", "own+", "Asserted"):
    try:
        lr.TypeRegime.parse(bad)
    except ValueError as e:
        assert "asserted" in str(e)
    else:
        raise AssertionError(bad)
assert value.as_turtle(regime=custom) == value.as_turtle(regime=lr.TypeRegime.parse("own+is_a"))
assert "directType" not in value.as_turtle(regime=custom) and "Asset" in value.as_turtle(regime=custom)

# as_turtle: default unchanged, IsA adds ancestor types and sesame:directType.
default = value.as_turtle()
assert default == value.as_turtle(skolem=False)
assert default == value.as_turtle(regime=lr.TypeRegime.ASSERTED)
assert "directType" not in default
isa = value.as_turtle(regime=lr.TypeRegime.IS_A)
assert "directType" in isa and "Asset" in isa and "Thing" in isa
assert "Placed" not in isa
assert "Placed" in value.as_turtle(regime=lr.TypeRegime.IS_A_AND_MIXINS)

# as_nquads: each partition lands in its own graph.
G = {p: "https://example.com/graphs/" + n for p, n in [
    (lr.TypePartition.Own, "own"), (lr.TypePartition.IsA, "isa"),
    (lr.TypePartition.Mixin, "mixin"), (lr.TypePartition.Direct, "direct"),
]}
nq = graphs_of(value.as_nquads(G, skolem=True))
assert set(nq) == set(G.values()), set(nq)
rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
s1 = "<" + NS + "s1>"
assert f"{s1} {rdf_type} <{NS}Signal>" in nq[G[lr.TypePartition.Own]]
assert f"{s1} {rdf_type} <{NS}Asset>" in nq[G[lr.TypePartition.IsA]]
assert f"{s1} {rdf_type} <{NS}Placed>" in nq[G[lr.TypePartition.Mixin]]
assert f"{s1} <{SESAME}> <{NS}Signal>" in nq[G[lr.TypePartition.Direct]]
assert all(f"<{SESAME}>" in t for t in nq[G[lr.TypePartition.Direct]])
assert not any(f"{rdf_type} <{NS}Asset>" in t for t in nq[G[lr.TypePartition.Own]])
# Partitions missing from the map are omitted.
only_own = graphs_of(value.as_nquads({lr.TypePartition.Own: G[lr.TypePartition.Own]}, skolem=True))
assert set(only_own) == {G[lr.TypePartition.Own]}
# The union of the graphs is the flat IS_A_AND_MIXINS output.
one_graph = graphs_of(value.as_nquads({p: "https://example.com/graphs/g" for p in G}, skolem=True))
assert list(one_graph) == ["https://example.com/graphs/g"]
assert one_graph["https://example.com/graphs/g"] == set().union(*nq.values())
flat_nt = io.BytesIO()
lr.export_ntriples(value, sv, flat_nt, skolem=True, regime=lr.TypeRegime.IS_A_AND_MIXINS)
assert {l[:-2] for l in nt_lines(flat_nt.getvalue().decode())} == one_graph["https://example.com/graphs/g"]

# An invalid graph IRI is a ValueError.
try:
    value.as_nquads({lr.TypePartition.Own: "not an iri"})
    raise AssertionError("expected ValueError")
except ValueError as e:
    assert "invalid graph IRI" in str(e), e

# Exporters write into a file-like object.
buf = io.BytesIO()
lr.export_turtle(value, sv, buf, regime=lr.TypeRegime.IS_A)
assert "directType" in buf.getvalue().decode()
buf = io.BytesIO()
lr.export_ntriples(value, sv, buf, skolem=True, regime=lr.TypeRegime.IS_A)
assert f"{s1} <{SESAME}> <{NS}Signal> .".encode() in buf.getvalue()
buf = io.BytesIO()
lr.export_nquads(value, sv, buf, G, skolem=True)
assert graphs_of(buf.getvalue().decode()) == nq
buf = io.BytesIO()
lr.export_trig(value, sv, buf, G, skolem=True)
trig = buf.getvalue().decode()
for g in G.values():
    assert "<" + g + ">" in trig
try:
    lr.export_trig(value, sv, io.BytesIO(), {lr.TypePartition.Own: "nope"})
    raise AssertionError("expected ValueError")
except ValueError:
    pass

# The importer reads regime output back as the same instance.
def import_back(ttl):
    items = list(lr.import_turtle(io.BytesIO(ttl.encode()), sv, ["Registry"]))
    assert len(items) == 1, items
    cls, inst = items[0]
    assert cls == "Registry"
    return inst.as_python()

original = value.as_python()
for regime in (lr.TypeRegime.ASSERTED, lr.TypeRegime.IS_A, lr.TypeRegime.IS_A_AND_MIXINS):
    assert import_back(value.as_turtle(skolem=True, regime=regime)) == original, regime
"#,
        )
        .unwrap();
        if let Err(err) = py.run(script.as_c_str(), Some(&locals), None) {
            panic!(
                "python script failed: {err}\n{}",
                err.traceback(py)
                    .map_or(String::new(), |t| t.format().unwrap_or_default())
            );
        }
    });
}

/// Importer failures reach Python as typed exceptions carrying their fields.
#[test]
fn import_errors_are_typed_python_exceptions() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let sys_modules = sys.getattr("modules").unwrap();
        let sys_modules = sys_modules.downcast::<PyDict>().unwrap();
        sys_modules.set_item("linkml_runtime", module).unwrap();

        let locals = PyDict::new(py);
        locals
            .set_item(
                "schema_path",
                data_path("type_regimes_import_schema.yaml")
                    .to_str()
                    .unwrap(),
            )
            .unwrap();
        locals
            .set_item("types_path", data_path("types.yaml").to_str().unwrap())
            .unwrap();

        let script = CString::new(
            r#"
import io
import linkml_runtime as lr

sv = lr.make_schema_view()
sv.add_schema_from_path(schema_path)
with open(types_path) as f:
    sv.add_schema_str_with_import_ref(f.read(), "https://example.org/regimes-import", "linkml:types")

PREFIXES = "@prefix ex: <http://example.org/> .\n@prefix sesame: <http://www.openrdf.org/schema/sesame#> .\n"

def run(body, roots=("Signal",)):
    ttl = PREFIXES + body
    return list(lr.import_turtle(io.BytesIO(ttl.encode()), sv, list(roots)))

def raised(body, roots=("Signal",)):
    try:
        run(body, roots)
    except Exception as e:
        return e
    raise AssertionError("expected an import error")

assert issubclass(lr.RdfImportError, Exception)
assert not issubclass(lr.RdfImportError, ImportError)
assert issubclass(lr.AmbiguousTypeError, lr.RdfImportError)
assert issubclass(lr.TypeConflictError, lr.RdfImportError)

# (a) two unrelated schema types, no directType
e = raised("ex:s1 a ex:Signal, ex:Pump .\n")
assert isinstance(e, lr.AmbiguousTypeError), type(e)
assert isinstance(e, lr.RdfImportError)
assert e.subject == "http://example.org/s1", e.subject
assert e.candidates == ["http://example.org/Pump", "http://example.org/Signal"], e.candidates
assert "Ambiguous type" in str(e) and "http://example.org/s1" in str(e), str(e)

# (b) rdf:type not implied by the directType
e = raised("ex:s2 a ex:Signal, ex:Pump ; sesame:directType ex:Signal .\n")
assert isinstance(e, lr.TypeConflictError), type(e)
assert isinstance(e, lr.RdfImportError)
assert e.subject == "http://example.org/s2", e.subject
assert e.direct_type == "http://example.org/Signal", e.direct_type
assert e.type_iri == "http://example.org/Pump", e.type_iri
assert "does not imply" in str(e), str(e)

# (c) any other import error is the base class, not a subclass
e = raised("ex:s3 a ex:Signal .\n", roots=("NoSuchClass",))
assert type(e) is lr.RdfImportError, type(e)
assert "NoSuchClass" in str(e), str(e)
"#,
        )
        .unwrap();
        if let Err(err) = py.run(script.as_c_str(), Some(&locals), None) {
            panic!(
                "python script failed: {err}\n{}",
                err.traceback(py)
                    .map_or(String::new(), |t| t.format().unwrap_or_default())
            );
        }
    });
}
