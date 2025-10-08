use linkml_meta::{ClassDefinition, EnumDefinition, SchemaDefinition, SlotDefinition};
use linkml_runtime::diff::{
    diff as diff_internal, patch as patch_internal, Delta, DeltaOp, DiffOptions, PatchTrace,
};
use linkml_runtime::turtle::{turtle_to_string, TurtleOptions};
use linkml_runtime::{load_json_str, load_yaml_str, LinkMLInstance, NodeId};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::io;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::{classview::ClassView, enumview::EnumView, slotview::SlotView};
use pyo3::conversion::{IntoPyObject, IntoPyObjectExt};
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::{PyAny, PyDict, PyModule, PyString, PyType};
use pyo3::Bound;
use pyo3::{wrap_pyfunction, wrap_pymodule};
#[cfg(feature = "stubgen")]
use pyo3_stub_gen::{
    define_stub_info_gatherer,
    derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods},
    PyStubType, TypeInfo,
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::Arc;
fn py_filelike_or_string_to_string(obj: &Bound<'_, PyAny>) -> PyResult<(String, Option<String>)> {
    if let Ok(s) = obj.extract::<String>() {
        if Path::new(&s).exists() {
            return fs::read_to_string(&s)
                .map_err(|e| PyException::new_err(e.to_string()))
                .map(|res| (res, Some(s)));
        }
        return Ok((s, None));
    }
    if obj.hasattr("__fspath__")? {
        let p: String = obj.call_method0("__fspath__")?.extract()?;
        return fs::read_to_string(&p)
            .map_err(|e| PyException::new_err(e.to_string()))
            .map(|res| (res, Some(p)));
    }
    if obj.hasattr("read")? {
        return obj.call_method0("read")?.extract().map(|res| (res, None));
    }
    Err(PyException::new_err("expected string or file-like object"))
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "SchemaView")]
pub struct PySchemaView {
    pub inner: Arc<SchemaView>,
}

impl PySchemaView {
    pub fn as_rust(&self) -> &SchemaView {
        self.inner.as_ref()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "ClassView")]
#[derive(Clone)]
pub struct PyClassView {
    inner: ClassView,
}

impl PyClassView {
    pub fn as_rust(&self) -> &ClassView {
        &self.inner
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "SlotView")]
#[derive(Clone)]
pub struct PySlotView {
    inner: SlotView,
}

impl PySlotView {
    pub fn as_rust(&self) -> &SlotView {
        &self.inner
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "EnumView")]
#[derive(Clone)]
pub struct PyEnumView {
    inner: EnumView,
}

impl PyEnumView {
    pub fn as_rust(&self) -> &EnumView {
        &self.inner
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PySchemaView {
    #[new]
    #[pyo3(signature = (source=None))]
    pub fn new(source: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut sv = SchemaView::new();
        if let Some(obj) = source {
            let (text, uri) = py_filelike_or_string_to_string(obj)?;
            let deser = serde_yml::Deserializer::from_str(&text);
            let schema: SchemaDefinition = serde_path_to_error::deserialize(deser)
                .map_err(|e| PyException::new_err(e.to_string()))?;
            sv.add_schema_with_import_ref(schema, uri.map(|u| ("".to_owned(), u)))
                .map_err(PyException::new_err)?;
        }
        Ok(Self {
            inner: Arc::new(sv),
        })
    }

    fn _get_resolved_schema_imports(&self) -> HashMap<(String, String), String> {
        self.inner._get_resolved_schema_imports()
    }

    fn get_default_prefix_for_schema(&self, schema_id: &str, expand: bool) -> Option<String> {
        self.inner.get_default_prefix_for_schema(schema_id, expand)
    }

    fn add_schema_from_path(&mut self, path: &str) -> PyResult<bool> {
        let schema =
            io::from_yaml(Path::new(path)).map_err(|e| PyException::new_err(e.to_string()))?;
        if let Some(inner) = std::sync::Arc::get_mut(&mut self.inner) {
            inner.add_schema(schema).map_err(PyException::new_err)
        } else {
            Err(PyException::new_err("SchemaView already shared"))
        }
    }

    fn add_schema_str(&mut self, data: &str) -> PyResult<bool> {
        let deser = serde_yml::Deserializer::from_str(data);
        let schema: SchemaDefinition = serde_path_to_error::deserialize(deser)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if let Some(inner) = std::sync::Arc::get_mut(&mut self.inner) {
            inner.add_schema(schema).map_err(PyException::new_err)
        } else {
            Err(PyException::new_err("SchemaView already shared"))
        }
    }

    fn get_unresolved_schemas(&self) -> Vec<String> {
        self.inner
            .get_unresolved_schemas()
            .iter()
            .map(|(_, uri)| uri.clone())
            .collect()
    }

    fn get_unresolved_schema_refs(&self) -> Vec<(String, String)> {
        self.inner.get_unresolved_schemas()
    }

    fn get_resolution_uri_of_schema(&self, id: &str) -> Option<String> {
        self.inner.get_resolution_uri_of_schema(id)
    }

    #[pyo3(text_signature = "(self, data, schema_id, uri)")]
    /// Add a schema payload that satisfies an unresolved import reference.
    ///
    /// Arguments
    /// ---------
    /// * `data` - The schema document as a YAML or JSON string.
    /// * `schema_id` - The identifier of the schema that declared the import needing this payload.
    /// * `uri` - The resolution URI (usually the import target) that produced the schema string.
    ///
    /// Example
    /// -------
    /// ```python
    /// from linkml_runtime import make_schema_view
    ///
    /// sv = make_schema_view()
    /// remote_text = '''imports:
    ///   - alias: personinfo
    ///     import_from: https://example.org/personinfo.yaml
    /// '''
    /// sv.add_schema_str_with_import_ref(
    ///     remote_text,
    ///     "personinfo",
    ///     "https://example.org/personinfo.yaml",
    /// )
    /// ```
    fn add_schema_str_with_import_ref(
        &mut self,
        data: &str,
        schema_id: &str,
        uri: &str,
    ) -> PyResult<()> {
        let deser = serde_yml::Deserializer::from_str(data);
        let schema: SchemaDefinition = serde_path_to_error::deserialize(deser)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if let Some(inner) = std::sync::Arc::get_mut(&mut self.inner) {
            inner
                .add_schema_with_import_ref(schema, Some((schema_id.to_string(), uri.to_string())))
                .map_err(PyException::new_err)?;
            Ok(())
        } else {
            Err(PyException::new_err("SchemaView already shared"))
        }
    }

    fn get_schema(&self, uri: &str) -> Option<SchemaDefinition> {
        self.inner.get_schema(uri).cloned()
    }

    #[classmethod]
    #[pyo3(name = "from_snapshot_yaml")]
    fn py_from_snapshot_yaml(_cls: &Bound<'_, PyType>, data: &str) -> PyResult<Self> {
        let view = SchemaView::from_snapshot_yaml(data)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(view),
        })
    }

    #[classmethod]
    #[pyo3(name = "from_snapshot_file")]
    fn py_from_snapshot_file(_cls: &Bound<'_, PyType>, path: &str) -> PyResult<Self> {
        let view = SchemaView::from_snapshot_file(path)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(view),
        })
    }

    fn to_snapshot_yaml(&self) -> PyResult<String> {
        self.inner
            .to_snapshot_yaml()
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn to_snapshot_file(&self, path: &str) -> PyResult<()> {
        self.inner
            .to_snapshot_file(path)
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn get_class_view(&self, id: &str) -> PyResult<Option<PyClassView>> {
        let conv = self.inner.converter();
        Ok(self
            .inner
            .get_class(&Identifier::new(id), &conv)
            .map_err(|e| PyException::new_err(format!("{:?}", e)))?
            .map(|cv| PyClassView { inner: cv }))
    }

    fn get_class_view_by_uri(&self, uri: &str) -> PyResult<Option<PyClassView>> {
        Ok(self
            .inner
            .get_class_by_uri(uri)
            .map_err(|e| PyException::new_err(format!("{:?}", e)))?
            .map(|cv| PyClassView { inner: cv }))
    }

    fn get_slot_view(&self, id: &str) -> PyResult<Option<PySlotView>> {
        let conv = self.inner.converter();
        Ok(self
            .inner
            .get_slot(&Identifier::new(id), &conv)
            .map_err(|e| PyException::new_err(format!("{:?}", e)))?
            .map(|svw| PySlotView { inner: svw }))
    }

    fn get_slot_view_by_uri(&self, uri: &str) -> PyResult<Option<PySlotView>> {
        Ok(self
            .inner
            .get_slot_by_uri(uri)
            .map_err(|e| PyException::new_err(format!("{:?}", e)))?
            .map(|svw| PySlotView { inner: svw }))
    }

    fn get_enum_view(&self, id: &str) -> PyResult<Option<PyEnumView>> {
        let conv = self.inner.converter();
        Ok(self
            .inner
            .get_enum(&Identifier::new(id), &conv)
            .map_err(|e| PyException::new_err(format!("{:?}", e)))?
            .map(|ev| PyEnumView { inner: ev }))
    }

    fn schema_ids(&self) -> Vec<String> {
        self.inner
            .all_schema_definitions()
            .map(|x| x.0.clone())
            .collect()
    }

    fn get_class_ids(&self) -> Vec<String> {
        self.inner.get_class_ids()
        /*
        let mut ids: HashSet<String> = HashSet::new();
        for (_, schema) in self.inner.iter_schemas() {
            if let Some(classes) = &schema.classes {
                classes.iter().map(|c| self.inner.get_uri(&schema.id, c.0)).for_each(|uri| {
                    ids.insert(uri.to_string());
                });
            }
        }
        ids.into_iter().collect()*/
    }

    fn get_slot_ids(&self) -> Vec<String> {
        self.inner.get_slot_ids() /*
                                  let mut ids: HashSet<String> = HashSet::new();
                                  for (_, schema) in self.inner.iter_schemas() {
                                      if let Some(slots) = &schema.slot_definitions {
                                          slots.iter().map(|c| self.inner.get_uri(&schema.id, c.0)).for_each(|uri| {
                                              ids.insert(uri.to_string());
                                          });
                                      }
                                  }
                                  ids.into_iter().collect()*/
    }

    fn class_views(&self) -> PyResult<Vec<PyClassView>> {
        self.inner
            .class_views()
            .map(|views| {
                views
                    .into_iter()
                    .map(|cv| PyClassView { inner: cv })
                    .collect()
            })
            .map_err(|e| PyException::new_err(format!("{:?}", e)))
    }

    fn enum_views(&self) -> PyResult<Vec<PyEnumView>> {
        self.inner
            .enum_views()
            .map(|views| {
                views
                    .into_iter()
                    .map(|ev| PyEnumView { inner: ev })
                    .collect()
            })
            .map_err(|e| PyException::new_err(format!("{:?}", e)))
    }

    fn slot_views(&self) -> PyResult<Vec<PySlotView>> {
        self.inner
            .slot_views()
            .map(|views| {
                views
                    .into_iter()
                    .map(|sv| PySlotView { inner: sv })
                    .collect()
            })
            .map_err(|e| PyException::new_err(format!("{:?}", e)))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "SchemaView(n_schemas={}, n_classes={}, n_slots={})",
            self.inner.all_schema_definitions().count(),
            self.inner.get_class_ids().len(),
            self.inner.get_slot_ids().len()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyClassView {
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[getter]
    pub fn definition(&self) -> ClassDefinition {
        self.inner.def().clone()
    }

    fn slots(&self) -> PyResult<Vec<PySlotView>> {
        Ok(self
            .inner
            .slots()
            .iter()
            .cloned()
            .map(|s| PySlotView { inner: s })
            .collect())
    }

    fn parent_class(&self) -> PyResult<Option<PyClassView>> {
        self.inner
            .parent_class()
            .map_err(|e| PyException::new_err(format!("{:?}", e)))
            .map(|opt| opt.map(|cv| PyClassView { inner: cv }))
    }

    fn identifier_slot(&self) -> Option<PySlotView> {
        self.inner
            .identifier_slot()
            .map(|sv| PySlotView { inner: sv.clone() })
    }

    fn key_or_identifier_slot(&self) -> Option<PySlotView> {
        self.inner
            .key_or_identifier_slot()
            .map(|sv| PySlotView { inner: sv.clone() })
    }

    fn get_descendants(&self, recurse: bool, include_mixins: bool) -> PyResult<Vec<PyClassView>> {
        self.inner
            .get_descendants(recurse, include_mixins)
            .map_err(|e| PyException::new_err(format!("{:?}", e)))
            .map(|v| v.into_iter().map(|cv| PyClassView { inner: cv }).collect())
    }

    fn schema_id(&self) -> String {
        self.inner.schema_id().to_string()
    }

    fn canonical_uri(&self) -> String {
        self.inner.canonical_uri().to_string()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "ClassView(name='{}', slots={})",
            self.inner.name(),
            self.inner.slots().len()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PySlotView {
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[getter]
    pub fn definition(&self) -> SlotDefinition {
        self.inner.definition().clone()
    }

    fn schema_id(&self) -> String {
        self.inner.schema_id().to_string()
    }

    fn canonical_uri(&self) -> String {
        self.inner.canonical_uri().to_string()
    }

    fn range_class(&self) -> Option<PyClassView> {
        self.inner
            .get_range_class()
            .map(|cv| PyClassView { inner: cv })
    }

    fn range_enum(&self) -> Option<PyEnumView> {
        self.inner
            .get_range_enum()
            .map(|ev| PyEnumView { inner: ev })
    }

    fn is_range_scalar(&self) -> bool {
        self.inner.is_range_scalar()
    }

    fn container_mode(&self) -> String {
        format!("{:?}", self.inner.determine_slot_container_mode())
    }

    fn inline_mode(&self) -> String {
        format!("{:?}", self.inner.determine_slot_inline_mode())
    }

    fn __repr__(&self) -> PyResult<String> {
        let range = self
            .inner
            .definition()
            .range
            .clone()
            .unwrap_or_else(|| "None".to_string());
        Ok(format!(
            "SlotView(name='{}', range='{}')",
            self.inner.name.clone(),
            range
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyEnumView {
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[getter]
    pub fn definition(&self) -> EnumDefinition {
        self.inner.definition().clone()
    }

    fn permissible_value_keys(&self) -> PyResult<Vec<String>> {
        self.inner
            .permissible_value_keys()
            .cloned()
            .map_err(|e| PyException::new_err(format!("{:?}", e)))
    }

    fn schema_id(&self) -> String {
        self.inner.schema_id().to_string()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "EnumView(name='{}', n_values={})",
            self.inner.name(),
            self.inner
                .permissible_value_keys()
                .map(|v| v.len())
                .unwrap_or(0)
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

#[pymodule(name = "linkml_schemaview")]
pub fn schemaview_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<PySchemaView>()?;
    m.add_class::<PyClassView>()?;
    m.add_class::<PySlotView>()?;
    m.add_class::<PyEnumView>()?;
    m.add_class::<SchemaDefinition>()?;
    m.add_class::<ClassDefinition>()?;
    m.add_class::<SlotDefinition>()?;
    m.add_class::<EnumDefinition>()?;
    Ok(())
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (source=None))]
fn make_schema_view(source: Option<&Bound<'_, PyAny>>) -> PyResult<PySchemaView> {
    PySchemaView::new(source)
}

/// Python bindings for `linkml_runtime`.
#[pymodule(name = "_native")]
pub fn runtime_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pymodule!(schemaview_module))?;
    m.add_function(wrap_pyfunction!(make_schema_view, m)?)?;
    m.add_function(wrap_pyfunction!(load_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(load_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_diff, m)?)?;
    m.add_function(wrap_pyfunction!(py_patch, m)?)?;
    m.add_function(wrap_pyfunction!(py_to_turtle, m)?)?;
    m.add_class::<PyLinkMLInstance>()?;
    m.add_class::<PyDelta>()?;
    m.add_class::<PyPatchTrace>()?;
    m.add_class::<PyPatchResult>()?;
    Ok(())
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "LinkMLInstance")]
pub struct PyLinkMLInstance {
    pub value: LinkMLInstance,
    pub sv: Py<PySchemaView>,
}

impl PyLinkMLInstance {
    pub fn new(value: LinkMLInstance, sv: Py<PySchemaView>) -> Self {
        Self { value, sv }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PyDeltaOp {
    Add,
    Remove,
    Update,
}

impl fmt::Display for PyDeltaOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PyDeltaOp::Add => "add",
            PyDeltaOp::Remove => "remove",
            PyDeltaOp::Update => "update",
        })
    }
}

impl From<PyDeltaOp> for DeltaOp {
    fn from(value: PyDeltaOp) -> Self {
        match value {
            PyDeltaOp::Add => DeltaOp::Add,
            PyDeltaOp::Remove => DeltaOp::Remove,
            PyDeltaOp::Update => DeltaOp::Update,
        }
    }
}

impl From<DeltaOp> for PyDeltaOp {
    fn from(value: DeltaOp) -> Self {
        match value {
            DeltaOp::Add => PyDeltaOp::Add,
            DeltaOp::Remove => PyDeltaOp::Remove,
            DeltaOp::Update => PyDeltaOp::Update,
        }
    }
}

impl<'py> FromPyObject<'py> for PyDeltaOp {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let raw: &str = ob.extract()?;
        match raw.to_ascii_lowercase().as_str() {
            "add" => Ok(PyDeltaOp::Add),
            "remove" => Ok(PyDeltaOp::Remove),
            "update" => Ok(PyDeltaOp::Update),
            _ => Err(PyValueError::new_err(format!(
                "invalid delta op '{}'; expected 'add', 'remove', or 'update'",
                raw
            ))),
        }
    }
}

impl<'py> IntoPyObject<'py> for PyDeltaOp {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let value = match self {
            PyDeltaOp::Add => "add",
            PyDeltaOp::Remove => "remove",
            PyDeltaOp::Update => "update",
        };
        Ok(PyString::new(py, value).into_any())
    }
}

#[cfg(feature = "stubgen")]
impl PyStubType for PyDeltaOp {
    fn type_output() -> TypeInfo {
        TypeInfo::with_module("typing.Literal['add', 'remove', 'update']", "typing".into())
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "Delta")]
#[derive(Clone)]
pub struct PyDelta {
    pub inner: Delta,
}

impl From<Delta> for PyDelta {
    fn from(delta: Delta) -> Self {
        Self { inner: delta }
    }
}

impl PyDelta {
    /// Convert any iterator of `Delta` values into owned Python handles.
    pub fn from_deltas<I>(py: Python<'_>, deltas: I) -> PyResult<Vec<Py<PyDelta>>>
    where
        I: IntoIterator<Item = Delta>,
    {
        deltas
            .into_iter()
            .map(|delta| Py::new(py, PyDelta::from(delta)))
            .collect()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyDelta {
    #[new]
    #[pyo3(signature = (path, op, old=None, new=None))]
    fn py_new(
        py: Python<'_>,
        path: Vec<String>,
        op: PyDeltaOp,
        old: Option<PyObject>,
        new: Option<PyObject>,
    ) -> PyResult<Self> {
        let old_value = match old {
            Some(obj) => Some(py_to_json_value(py, obj.bind(py))?),
            None => None,
        };
        let new_value = match new {
            Some(obj) => Some(py_to_json_value(py, obj.bind(py))?),
            None => None,
        };
        Ok(Self {
            inner: Delta {
                path,
                op: op.into(),
                old: old_value,
                new: new_value,
            },
        })
    }

    #[getter]
    fn path(&self) -> Vec<String> {
        self.inner.path.clone()
    }

    #[getter]
    fn op(&self) -> PyDeltaOp {
        self.inner.op.clone().into()
    }

    #[getter]
    fn old<'py>(&self, py: Python<'py>) -> PyObject {
        match &self.inner.old {
            Some(v) => json_value_to_py(py, v),
            None => py.None(),
        }
    }

    #[getter]
    #[allow(clippy::wrong_self_convention, clippy::new_ret_no_self)]
    fn new<'py>(&self, py: Python<'py>) -> PyObject {
        match &self.inner.new {
            Some(v) => json_value_to_py(py, v),
            None => py.None(),
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        let op_label = self.op();
        let old_repr = self
            .inner
            .old
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "None".to_string());
        let new_repr = self
            .inner
            .new
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "None".to_string());
        Ok(format!(
            "Delta(op='{}', path={:?}, old={}, new={})",
            op_label, self.inner.path, old_repr, new_repr
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("path", self.inner.path.clone())?;
        dict.set_item("op", self.op())?;
        dict.set_item("old", self.old(py))?;
        dict.set_item("new", self.new(py))?;
        Ok(dict.into())
    }
}

fn json_value_to_py(py: Python<'_>, v: &JsonValue) -> PyObject {
    let s = serde_json::to_string(v).unwrap();
    let json_mod = PyModule::import(py, "json").unwrap();
    json_mod.call_method1("loads", (s,)).unwrap().unbind()
}

fn py_to_json_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    let json_mod = PyModule::import(py, "json")?;
    let s: String = json_mod.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&s).map_err(|e| PyException::new_err(e.to_string()))
}

/// Convert a mapping of `NodeId` keys into a Python dictionary.
pub fn node_map_into_pydict<V, I>(py: Python<'_>, entries: I) -> PyResult<Py<PyDict>>
where
    I: IntoIterator<Item = (NodeId, V)>,
    V: for<'py> IntoPyObject<'py>,
{
    let dict = PyDict::new(py);
    for (node_id, value) in entries {
        dict.set_item(node_id, value.into_py_any(py)?)?;
    }
    Ok(dict.into())
}

impl Clone for PyLinkMLInstance {
    fn clone(&self) -> Self {
        Python::with_gil(|py| Self {
            value: self.value.clone(),
            sv: self.sv.clone_ref(py),
        })
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyLinkMLInstance {
    /// Semantic equality per LinkML Instances spec.
    /// Compares this value with another `LinkMLInstance`.
    #[pyo3(signature = (other, treat_missing_as_null = false))]
    fn equals(&self, other: &PyLinkMLInstance, treat_missing_as_null: bool) -> bool {
        self.value.equals(&other.value, treat_missing_as_null)
    }
    #[getter]
    fn slot_name(&self) -> Option<String> {
        self.value.slot().map(|slot| slot.name.clone())
    }

    #[getter]
    fn kind(&self) -> String {
        match &self.value {
            LinkMLInstance::Scalar { .. } => "scalar".to_string(),
            LinkMLInstance::Null { .. } => "null".to_string(),
            LinkMLInstance::List { .. } => "list".to_string(),
            LinkMLInstance::Mapping { .. } => "mapping".to_string(),
            LinkMLInstance::Object { .. } => "object".to_string(),
        }
    }

    #[getter]
    fn schema_view<'py>(&self, py: Python<'py>) -> Py<PySchemaView> {
        self.sv.clone_ref(py)
    }

    #[getter]
    fn node_id(&self) -> u64 {
        self.value.node_id()
    }

    #[getter]
    fn slot_definition(&self) -> Option<SlotDefinition> {
        match &self.value {
            LinkMLInstance::Scalar { slot, .. } => Some(slot.definition().clone()),
            LinkMLInstance::List { slot, .. } => Some(slot.definition().clone()),
            LinkMLInstance::Null { slot, .. } => Some(slot.definition().clone()),
            _ => None,
        }
    }

    #[getter]
    fn class_definition(&self) -> Option<ClassDefinition> {
        match &self.value {
            LinkMLInstance::Object { class, .. } => Some(class.def().clone()),
            LinkMLInstance::Scalar { class: Some(c), .. } => Some(c.def().clone()),
            LinkMLInstance::List { class: Some(c), .. } => Some(c.def().clone()),
            LinkMLInstance::Null { class: Some(c), .. } => Some(c.def().clone()),
            _ => None,
        }
    }

    #[getter]
    fn class_name(&self) -> Option<String> {
        match &self.value {
            LinkMLInstance::Object { class, .. } => Some(class.def().name.clone()),
            LinkMLInstance::Scalar { class: Some(c), .. } => Some(c.def().name.clone()),
            LinkMLInstance::List { class: Some(c), .. } => Some(c.def().name.clone()),
            LinkMLInstance::Null { class: Some(c), .. } => Some(c.def().name.clone()),
            _ => None,
        }
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(match &self.value {
            LinkMLInstance::Scalar { .. } => 0,
            LinkMLInstance::Null { .. } => 0,
            LinkMLInstance::List { values, .. } => values.len(),
            LinkMLInstance::Mapping { values, .. } => values.len(),
            LinkMLInstance::Object { values, .. } => values.len(),
        })
    }

    fn __getitem__<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyLinkMLInstance> {
        match &self.value {
            LinkMLInstance::List { values, .. } => {
                let idx: usize = key.extract()?;
                values
                    .get(idx)
                    .map(|v| PyLinkMLInstance::new(v.clone(), self.sv.clone_ref(py)))
                    .ok_or_else(|| PyException::new_err("index out of range"))
            }
            LinkMLInstance::Object { values, .. } => {
                let k: String = key.extract()?;
                values
                    .get(&k)
                    .map(|v| PyLinkMLInstance::new(v.clone(), self.sv.clone_ref(py)))
                    .ok_or_else(|| PyException::new_err("key not found"))
            }
            LinkMLInstance::Mapping { values, .. } => {
                let k: String = key.extract()?;
                values
                    .get(&k)
                    .map(|v| PyLinkMLInstance::new(v.clone(), self.sv.clone_ref(py)))
                    .ok_or_else(|| PyException::new_err("key not found"))
            }
            _ => Err(PyException::new_err("not indexable")),
        }
    }

    /// Navigate by a path of strings (map keys or list indices).
    /// Returns a new LinkMLInstance if found, otherwise None.
    #[pyo3(name = "navigate")]
    fn py_navigate<'py>(
        &self,
        py: Python<'py>,
        path: &Bound<'py, PyAny>,
    ) -> PyResult<Option<PyLinkMLInstance>> {
        // Expect any iterable of strings
        let path_vec: Vec<String> = path
            .extract()
            .map_err(|_| PyException::new_err("path must be a sequence of strings"))?;
        if let Some(found) = self.value.navigate_path(&path_vec) {
            Ok(Some(PyLinkMLInstance::new(
                found.clone(),
                self.sv.clone_ref(py),
            )))
        } else {
            Ok(None)
        }
    }

    fn keys(&self) -> PyResult<Vec<String>> {
        match &self.value {
            LinkMLInstance::Object { values, .. } => Ok(values.keys().cloned().collect()),
            LinkMLInstance::Mapping { values, .. } => Ok(values.keys().cloned().collect()),
            _ => Ok(Vec::new()),
        }
    }

    fn values<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyLinkMLInstance>> {
        match &self.value {
            LinkMLInstance::Object { values, .. } => Ok(values
                .values()
                .cloned()
                .map(|v| PyLinkMLInstance::new(v, self.sv.clone_ref(py)))
                .collect()),
            LinkMLInstance::Mapping { values, .. } => Ok(values
                .values()
                .cloned()
                .map(|v| PyLinkMLInstance::new(v, self.sv.clone_ref(py)))
                .collect()),
            LinkMLInstance::List { values, .. } => Ok(values
                .iter()
                .cloned()
                .map(|v| PyLinkMLInstance::new(v, self.sv.clone_ref(py)))
                .collect()),
            _ => Ok(Vec::new()),
        }
    }

    fn as_python<'py>(&self, py: Python<'py>) -> PyObject {
        json_value_to_py(py, &self.value.to_json())
    }

    fn as_turtle<'py>(&self, py: Python<'py>, skolem: Option<bool>) -> PyResult<String> {
        let sv_ref = self.sv.bind(py).borrow();
        let rust_sv = sv_ref.as_rust();
        let schema = rust_sv
            .primary_schema()
            .ok_or_else(|| PyException::new_err("no schema loaded"))?;
        let conv = rust_sv.converter();
        turtle_to_string(
            &self.value,
            rust_sv,
            schema,
            &conv,
            TurtleOptions {
                skolem: skolem.unwrap_or(false),
            },
        )
        .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(match &self.value {
            LinkMLInstance::Scalar { value, slot, .. } => {
                format!(
                    "LinkMLInstance.Scalar(slot='{}', value={})",
                    slot.name, value
                )
            }
            LinkMLInstance::Null { slot, .. } => {
                format!("LinkMLInstance.Null(slot='{}')", slot.name)
            }
            LinkMLInstance::List { values, slot, .. } => {
                format!(
                    "LinkMLInstance.List(slot='{}', len={})",
                    slot.name,
                    values.len()
                )
            }
            LinkMLInstance::Mapping { values, slot, .. } => {
                format!(
                    "LinkMLInstance.Mapping(slot='{}', keys={:?})",
                    slot.name,
                    values.keys().collect::<Vec<&String>>()
                )
            }
            LinkMLInstance::Object { values, class, .. } => {
                let keys: Vec<&String> = values.keys().collect();
                format!(
                    "LinkMLInstance.Object(class='{}', keys={:?})",
                    class.def().name.clone(),
                    keys
                )
            }
        })
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "PatchTrace")]
pub struct PyPatchTrace {
    added: Vec<u64>,
    deleted: Vec<u64>,
    updated: Vec<u64>,
    failed: Vec<Vec<String>>,
}

impl From<PatchTrace> for PyPatchTrace {
    fn from(trace: PatchTrace) -> Self {
        Self {
            added: trace.added,
            deleted: trace.deleted,
            updated: trace.updated,
            failed: trace.failed,
        }
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyPatchTrace {
    #[getter]
    fn added(&self) -> Vec<u64> {
        self.added.clone()
    }

    #[getter]
    fn deleted(&self) -> Vec<u64> {
        self.deleted.clone()
    }

    #[getter]
    fn updated(&self) -> Vec<u64> {
        self.updated.clone()
    }

    #[getter]
    fn failed(&self) -> Vec<Vec<String>> {
        self.failed.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "PatchTrace(added={:?}, deleted={:?}, updated={:?}, failed={:?})",
            self.added, self.deleted, self.updated, self.failed
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "PatchResult")]
pub struct PyPatchResult {
    value: Py<PyLinkMLInstance>,
    trace: Py<PyPatchTrace>,
}

impl PyPatchResult {
    fn new(value: Py<PyLinkMLInstance>, trace: Py<PyPatchTrace>) -> Self {
        Self { value, trace }
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyPatchResult {
    #[getter]
    fn value(&self, py: Python<'_>) -> Py<PyLinkMLInstance> {
        self.value.clone_ref(py)
    }

    #[getter]
    fn trace(&self, py: Python<'_>) -> Py<PyPatchTrace> {
        self.trace.clone_ref(py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let value_repr: String = self.value.bind(py).repr()?.extract()?;
        let trace_repr: String = self.trace.bind(py).repr()?.extract()?;
        Ok(format!(
            "PatchResult(value={}, trace={})",
            value_repr, trace_repr
        ))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(signature = (source, sv, class_view))]
fn load_yaml(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    sv: Py<PySchemaView>,
    class_view: Py<PyClassView>,
) -> PyResult<PyLinkMLInstance> {
    let sv_ref = sv.bind(py).borrow();
    let rust_sv = sv_ref.as_rust();
    let conv = rust_sv.converter();
    let class_bound = class_view.bind(py);
    let class_ref = class_bound.borrow();
    let (text, _) = py_filelike_or_string_to_string(source)?;
    let v = load_yaml_str(&text, rust_sv, class_ref.as_rust(), &conv)
        .map_err(|e| PyException::new_err(e.to_string()))?;
    Ok(PyLinkMLInstance::new(v, sv))
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(signature = (source, sv, class_view))]
fn load_json(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    sv: Py<PySchemaView>,
    class_view: Py<PyClassView>,
) -> PyResult<PyLinkMLInstance> {
    let sv_ref = sv.bind(py).borrow();
    let rust_sv = sv_ref.as_rust();
    let conv = rust_sv.converter();
    let class_bound = class_view.bind(py);
    let class_ref = class_bound.borrow();
    let (text, _) = py_filelike_or_string_to_string(source)?;
    let v = load_json_str(&text, rust_sv, class_ref.as_rust(), &conv)
        .map_err(|e| PyException::new_err(e.to_string()))?;
    Ok(PyLinkMLInstance::new(v, sv))
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(
    name = "diff",
    signature = (
        source,
        target,
        treat_missing_as_null = false,
        treat_changed_identifier_as_new_object = true
    ),
    text_signature = "(source, target, treat_missing_as_null=False, treat_changed_identifier_as_new_object=True)"
)]
/// Compute deltas between two instances.
///
/// Defaults mirror `linkml_runtime.diff.DiffOptions::default()`: missing assignments are
/// treated as absent (`treat_missing_as_null=False`) and identifier changes are emitted as
/// whole-object replacements (`treat_changed_identifier_as_new_object=True`).
fn py_diff(
    py: Python<'_>,
    source: &PyLinkMLInstance,
    target: &PyLinkMLInstance,
    treat_missing_as_null: bool,
    treat_changed_identifier_as_new_object: bool,
) -> PyResult<Vec<Py<PyDelta>>> {
    let opts = DiffOptions {
        treat_missing_as_null,
        treat_changed_identifier_as_new_object,
    };
    let deltas = diff_internal(&source.value, &target.value, opts);
    deltas
        .into_iter()
        .map(|delta| Py::new(py, PyDelta::from(delta)))
        .collect()
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "patch", signature = (source, deltas, treat_missing_as_null = true, ignore_no_ops = true))]
fn py_patch(
    py: Python<'_>,
    source: &PyLinkMLInstance,
    deltas: Vec<Py<PyDelta>>,
    treat_missing_as_null: bool,
    ignore_no_ops: bool,
) -> PyResult<Py<PyPatchResult>> {
    let mut deltas_vec = Vec::with_capacity(deltas.len());
    for delta in deltas {
        let bound = delta.bind(py);
        deltas_vec.push(bound.borrow().inner.clone());
    }
    let (new_value, trace) = patch_internal(
        &source.value,
        &deltas_vec,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops,
            treat_missing_as_null,
        },
    )
    .map_err(|e| PyException::new_err(e.to_string()))?;
    let py_val = Py::new(
        py,
        PyLinkMLInstance::new(new_value, source.sv.clone_ref(py)),
    )?;
    let py_trace = Py::new(py, PyPatchTrace::from(trace))?;
    let result = PyPatchResult::new(py_val, py_trace);
    Py::new(py, result)
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "to_turtle", signature = (value, skolem=None))]
fn py_to_turtle(
    py: Python<'_>,
    value: &PyLinkMLInstance,
    skolem: Option<bool>,
) -> PyResult<String> {
    value.as_turtle(py, skolem)
}

#[cfg(feature = "stubgen")]
define_stub_info_gatherer!(stub_info);
