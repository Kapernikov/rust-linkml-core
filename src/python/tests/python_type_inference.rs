//! Python surface of the RDF type inference: `inference=` on the Turtle writers,
//! quad output per type partition, and the importer reading inference output back.
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
fn type_inference_via_python() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let sys_modules = sys.getattr("modules").unwrap();
        let sys_modules = sys_modules.cast::<PyDict>().unwrap();
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

# Inference as a set of partitions.
assert list(lr.TypeInference.IS_A_AND_MIXINS) == [
    lr.TypePartition.OWN, lr.TypePartition.IS_A, lr.TypePartition.MIXIN, lr.TypePartition.DIRECT,
]
assert list(lr.TypeInference.ASSERTED) == [lr.TypePartition.OWN]
assert list(lr.TypeInference.IS_A) == [
    lr.TypePartition.OWN, lr.TypePartition.IS_A, lr.TypePartition.DIRECT,
]

P = lr.TypePartition
custom = lr.TypeInference([P.IS_A, P.OWN])
assert list(custom) == [P.OWN, P.IS_A]
assert custom == lr.TypeInference.parse("{own,is_a}") == lr.TypeInference(iter([P.OWN, P.IS_A, P.OWN]))
assert str(custom) == "{own,is_a}" and repr(custom) == "TypeInference.parse('{own,is_a}')"
assert str(lr.TypeInference([P.IS_A])) == "{is_a}"
assert lr.TypeInference.parse("{is_a}") == lr.TypeInference([P.IS_A]) != lr.TypeInference.IS_A
assert str(lr.TypeInference.IS_A) == "is_a" and repr(lr.TypeInference.IS_A) == "TypeInference.IS_A"
assert repr(lr.TypeInference.ASSERTED) == "TypeInference.ASSERTED"
assert lr.TypeInference.parse("is_a_and_mixins") == lr.TypeInference.IS_A_AND_MIXINS
assert str(lr.TypeInference([])) == "{}" and lr.TypeInference.parse("{}") == lr.TypeInference([])
assert lr.TypeInference.parse(" { direct , own, own } ") == lr.TypeInference([P.OWN, P.DIRECT])
assert lr.TypeInference.parse("{own}") == lr.TypeInference.ASSERTED
assert str(lr.TypeInference.parse("{own}")) == "asserted"
# Constructor: a single partition or any iterable of them.
assert lr.TypeInference(P.OWN) == lr.TypeInference.ASSERTED == lr.TypeInference([P.OWN])
try:
    lr.TypeInference(7)
except TypeError:
    pass
else:
    raise AssertionError("TypeInference(7)")
# Iteration, len, subset, union and intersection.
assert list(lr.TypeInference.IS_A) == [P.OWN, P.IS_A, P.DIRECT]
assert list(lr.TypeInference([])) == [] and len(lr.TypeInference([])) == 0
assert len(lr.TypeInference.IS_A_AND_MIXINS) == 4 and len(lr.TypeInference.ASSERTED) == 1
assert lr.TypeInference.ASSERTED <= lr.TypeInference.IS_A <= lr.TypeInference.IS_A_AND_MIXINS
assert not (lr.TypeInference.IS_A_AND_MIXINS <= lr.TypeInference.IS_A)
assert lr.TypeInference.IS_A >= lr.TypeInference.ASSERTED and lr.TypeInference([]) <= lr.TypeInference([])
assert lr.TypeInference.IS_A & lr.TypeInference.IS_A_AND_MIXINS == lr.TypeInference.IS_A
assert lr.TypeInference.IS_A & P.MIXIN == lr.TypeInference([])
assert lr.TypeInference.IS_A & P.IS_A == lr.TypeInference([P.IS_A])
assert P.IS_A & lr.TypeInference.IS_A == lr.TypeInference([P.IS_A])
assert lr.TypeInference.ASSERTED | P.IS_A == lr.TypeInference([P.OWN, P.IS_A])
assert P.IS_A | lr.TypeInference.ASSERTED == lr.TypeInference([P.OWN, P.IS_A])
assert lr.TypeInference.ASSERTED | lr.TypeInference([P.IS_A, P.DIRECT]) == lr.TypeInference.IS_A
assert P.MIXIN in lr.TypeInference.IS_A_AND_MIXINS and P.MIXIN not in lr.TypeInference.IS_A
assert lr.TypeInference.IS_A != lr.TypeInference.IS_A_AND_MIXINS
seen = {lr.TypeInference.IS_A: "a", lr.TypeInference([P.OWN, P.IS_A, P.DIRECT]): "b"}
assert seen == {lr.TypeInference.IS_A: "b"}
# TypePartition: str, repr, parse.
assert [str(p) for p in (P.OWN, P.IS_A, P.MIXIN, P.DIRECT)] == ["own", "is_a", "mixin", "direct"]
assert repr(P.IS_A) == "TypePartition.IS_A" and repr(P.OWN) == "TypePartition.OWN"
for p in (P.OWN, P.IS_A, P.MIXIN, P.DIRECT):
    assert P.parse(str(p)) == p
for bad in ("isa", "Own", "", "{own}"):
    try:
        P.parse(bad)
    except ValueError:
        pass
    else:
        raise AssertionError(bad)
for bad in ("bogus", "own+is_a", "{own,bogus}", "is_a}", "none", "Asserted"):
    try:
        lr.TypeInference.parse(bad)
    except ValueError as e:
        assert "asserted" in str(e)
    else:
        raise AssertionError(bad)
assert value.as_turtle(inference=custom) == value.as_turtle(inference=lr.TypeInference.parse("{own,is_a}"))
assert "directType" not in value.as_turtle(inference=custom) and "Asset" in value.as_turtle(inference=custom)

# as_turtle: default unchanged, IsA adds ancestor types and sesame:directType.
default = value.as_turtle()
assert default == value.as_turtle(skolem=False)
assert default == value.as_turtle(inference=lr.TypeInference.ASSERTED)
assert "directType" not in default
isa = value.as_turtle(inference=lr.TypeInference.IS_A)
assert "directType" in isa and "Asset" in isa and "Thing" in isa
assert "Placed" not in isa
assert "Placed" in value.as_turtle(inference=lr.TypeInference.IS_A_AND_MIXINS)

# as_nquads: each partition lands in its own graph.
G = {p: "https://example.com/graphs/" + n for p, n in [
    (lr.TypePartition.OWN, "own"), (lr.TypePartition.IS_A, "is_a"),
    (lr.TypePartition.MIXIN, "mixin"), (lr.TypePartition.DIRECT, "direct"),
]}
nq = graphs_of(value.as_nquads(G, skolem=True))
assert set(nq) == set(G.values()), set(nq)
rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
s1 = "<" + NS + "s1>"
assert f"{s1} {rdf_type} <{NS}Signal>" in nq[G[lr.TypePartition.OWN]]
assert f"{s1} {rdf_type} <{NS}Asset>" in nq[G[lr.TypePartition.IS_A]]
assert f"{s1} {rdf_type} <{NS}Placed>" in nq[G[lr.TypePartition.MIXIN]]
assert f"{s1} <{SESAME}> <{NS}Signal>" in nq[G[lr.TypePartition.DIRECT]]
assert all(f"<{SESAME}>" in t for t in nq[G[lr.TypePartition.DIRECT]])
assert not any(f"{rdf_type} <{NS}Asset>" in t for t in nq[G[lr.TypePartition.OWN]])
# Partitions missing from the map are omitted.
only_own = graphs_of(value.as_nquads({lr.TypePartition.OWN: G[lr.TypePartition.OWN]}, skolem=True))
assert set(only_own) == {G[lr.TypePartition.OWN]}
# The union of the graphs is the flat IS_A_AND_MIXINS output.
one_graph = graphs_of(value.as_nquads({p: "https://example.com/graphs/g" for p in G}, skolem=True))
assert list(one_graph) == ["https://example.com/graphs/g"]
assert one_graph["https://example.com/graphs/g"] == set().union(*nq.values())
flat_nt = io.BytesIO()
lr.export_ntriples(value, sv, flat_nt, skolem=True, inference=lr.TypeInference.IS_A_AND_MIXINS)
assert {l[:-2] for l in nt_lines(flat_nt.getvalue().decode())} == one_graph["https://example.com/graphs/g"]

# None routes a partition to the default graph; as_trig takes the same mapping.
mixed = {lr.TypePartition.OWN: None, lr.TypePartition.DIRECT: G[lr.TypePartition.DIRECT]}
mixed_nq = value.as_nquads(mixed, skolem=True)
direct_graph = G[lr.TypePartition.DIRECT]
named_suffix = " <" + direct_graph + "> ."
named_lines = {l for l in nt_lines(mixed_nq) if l.endswith(named_suffix)}
default_lines = nt_lines(mixed_nq) - named_lines
asserted_nt = io.BytesIO()
lr.export_ntriples(value, sv, asserted_nt, skolem=True)
assert default_lines == nt_lines(asserted_nt.getvalue().decode()), "Own goes to the default graph"
assert named_lines and all(f"<{SESAME}>" in l for l in named_lines)
trig_text = value.as_trig(mixed, skolem=True)
assert "@prefix" in trig_text and G[lr.TypePartition.DIRECT] in trig_text
for g in G.values():
    assert g in value.as_trig(G, skolem=True)
# Flat Turtle has no graph names; an empty mapping is a ValueError.
try:
    value.as_trig({})
    raise AssertionError("expected ValueError")
except ValueError as e:
    assert "empty" in str(e), e

# skolem (and inference) are keyword-only on as_turtle; skolem on the quad writers too.
for call in (lambda: value.as_turtle(True), lambda: value.as_nquads(G, True), lambda: value.as_trig(G, True)):
    try:
        call()
        raise AssertionError("expected TypeError")
    except TypeError:
        pass
assert value.as_turtle(skolem=True, inference=lr.TypeInference.IS_A) == value.as_turtle(inference=lr.TypeInference.IS_A, skolem=True)

# An invalid graph IRI is a ValueError.
try:
    value.as_nquads({lr.TypePartition.OWN: "not an iri"})
    raise AssertionError("expected ValueError")
except ValueError as e:
    assert "invalid graph IRI" in str(e), e

# Exporters write into a file-like object.
buf = io.BytesIO()
lr.export_turtle(value, sv, buf, inference=lr.TypeInference.IS_A)
assert "directType" in buf.getvalue().decode()
buf = io.BytesIO()
lr.export_ntriples(value, sv, buf, skolem=True, inference=lr.TypeInference.IS_A)
assert f"{s1} <{SESAME}> <{NS}Signal> .".encode() in buf.getvalue()
buf = io.BytesIO()
lr.export_nquads(value, sv, buf, G, skolem=True)
assert graphs_of(buf.getvalue().decode()) == nq
buf = io.BytesIO()
lr.export_trig(value, sv, buf, G, skolem=True)
trig = buf.getvalue().decode()
for g in G.values():
    assert "<" + g + ">" in trig

# An iterable is consumed as it is written, so an error it raises mid-way
# reaches the caller unchanged.
def failing():
    yield value
    raise RuntimeError("source failed")
try:
    lr.export_ntriples(failing(), sv, io.BytesIO())
    raise AssertionError("expected RuntimeError")
except RuntimeError as e:
    assert "source failed" in str(e), e
try:
    lr.export_trig(value, sv, io.BytesIO(), {lr.TypePartition.OWN: "nope"})
    raise AssertionError("expected ValueError")
except ValueError:
    pass

# The importer reads inference output back as the same instance.
def import_back(ttl):
    items = list(lr.import_turtle(io.BytesIO(ttl.encode()), sv, ["Registry"]))
    assert len(items) == 1, items
    cls, inst = items[0]
    assert cls == "Registry"
    return inst.as_python()

original = value.as_python()
for inference in (lr.TypeInference.ASSERTED, lr.TypeInference.IS_A, lr.TypeInference.IS_A_AND_MIXINS):
    assert import_back(value.as_turtle(skolem=True, inference=inference)) == original, inference
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
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let sys = py.import("sys").unwrap();
        let sys_modules = sys.getattr("modules").unwrap();
        let sys_modules = sys_modules.cast::<PyDict>().unwrap();
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
assert issubclass(lr.ConflictingDirectTypesError, lr.RdfImportError)
assert not issubclass(lr.ConflictingDirectTypesError, lr.AmbiguousTypeError)

# (a) two unrelated schema types, no directType
e = raised("ex:s1 a ex:Signal, ex:Pump .\n")
assert isinstance(e, lr.AmbiguousTypeError), type(e)
assert isinstance(e, lr.RdfImportError)
assert e.subject == "http://example.org/s1", e.subject
assert e.candidates == ["http://example.org/Pump", "http://example.org/Signal"], e.candidates
assert "Ambiguous type" in str(e) and "http://example.org/s1" in str(e), str(e)

# (a2) several directType values: its own exception, a sibling of AmbiguousTypeError
e = raised("ex:s4 a ex:Signal, ex:Pump ; sesame:directType ex:Signal, ex:Pump .\n")
assert type(e) is lr.ConflictingDirectTypesError, type(e)
assert isinstance(e, lr.RdfImportError) and not isinstance(e, lr.AmbiguousTypeError)
assert e.subject == "http://example.org/s4", e.subject
assert e.direct_types == ["http://example.org/Pump", "http://example.org/Signal"], e.direct_types
assert not hasattr(e, "candidates")
assert "keep one sesame:directType" in str(e), str(e)

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

/// A downstream binding converts Rust inferences and partitions to their Python
/// classes directly, without a string round trip.
#[test]
fn rust_inferences_and_partitions_convert_to_python() {
    use linkml_runtime::schema_rdf::{TypeInference, TypePartition};
    use linkml_runtime_python::{PyTypeInference, PyTypePartition};

    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "linkml_runtime").unwrap();
        runtime_module(&module).unwrap();
        let inference = Py::new(py, PyTypeInference::from(TypeInference::IS_A)).unwrap();
        let named = module
            .getattr("TypeInference")
            .unwrap()
            .getattr("IS_A")
            .unwrap();
        assert!(inference.bind(py).as_any().eq(&named).unwrap());
        assert_eq!(inference.bind(py).str().unwrap().to_string(), "is_a");

        let custom: TypeInference = [TypePartition::Own, TypePartition::IsA]
            .into_iter()
            .collect();
        let back: TypeInference = PyTypeInference::from(custom).into();
        assert_eq!(back, custom);

        let partition = Py::new(py, PyTypePartition::from(TypePartition::Mixin)).unwrap();
        let named = module
            .getattr("TypePartition")
            .unwrap()
            .getattr("MIXIN")
            .unwrap();
        assert!(partition.bind(py).as_any().eq(&named).unwrap());
    });
}
