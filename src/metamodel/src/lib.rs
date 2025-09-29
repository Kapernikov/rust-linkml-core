#![allow(non_camel_case_types)]

pub mod poly;
pub mod poly_containers;
#[cfg(feature = "serde")]
mod serde_utils;

use chrono::NaiveDateTime;
use merge::Merge;
#[cfg(feature = "pyo3")]
use pyo3::{prelude::*, FromPyObject};
#[cfg(feature = "stubgen")]
use pyo3_stub_gen::{
    define_stub_info_gatherer, derive::gen_stub_pyclass, derive::gen_stub_pymethods,
};
#[cfg(feature = "serde")]
use serde::{de::IntoDeserializer, Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde_path_to_error;
use serde_value::Value;
#[cfg(feature = "serde")]
use serde_yml as _;
use std::collections::BTreeMap;
use std::collections::HashMap;

// Types

pub type string = String;
pub type integer = String;
pub type boolean = String;
pub type float = f64;
pub type double = f64;
pub type decimal = String;
pub type time = String;
pub type date = String;
pub type datetime = String;
pub type date_or_datetime = String;
pub type uriorcurie = String;
pub type curie = String;
pub type uri = String;
pub type ncname = String;
pub type objectidentifier = String;
pub type nodeidentifier = String;
pub type jsonpointer = String;
pub type jsonpath = String;
pub type sparqlpath = String;

// Slots

pub type mappings = Vec<uriorcurie>;
pub type exact_mappings = Vec<uriorcurie>;
pub type close_mappings = Vec<uriorcurie>;
pub type related_mappings = Vec<uriorcurie>;
pub type narrow_mappings = Vec<uriorcurie>;
pub type broad_mappings = Vec<uriorcurie>;
pub type deprecated_element_has_exact_replacement = uriorcurie;
pub type deprecated_element_has_possible_replacement = uriorcurie;
pub type extensions = Vec<Extension>;
pub type extension_tag = uriorcurie;
pub type extension_value = AnyValue;
pub type annotations = Vec<Annotation>;
pub type unit = UnitOfMeasure;
pub type ucum_code = String;
pub type derivation = String;
pub type has_quantity_kind = uriorcurie;
pub type iec61360code = String;
pub type symbol = String;
pub type abbreviation = String;
pub type descriptive_name = String;
pub type name = String;
pub type title = String;
pub type conforms_to = String;
pub type implements = Vec<uriorcurie>;
pub type instantiates = Vec<uriorcurie>;
pub type categories = Vec<uriorcurie>;
pub type keywords = Vec<String>;
pub type definition_uri = uriorcurie;
pub type id_prefixes = Vec<ncname>;
pub type id_prefixes_are_closed = bool;
pub type description = String;
pub type structured_aliases = Vec<StructuredAlias>;
pub type aliases = Vec<String>;
pub type deprecated = String;
pub type todos = Vec<String>;
pub type notes = Vec<String>;
pub type comments = Vec<String>;
pub type in_subset = Vec<SubsetDefinition>;
pub type from_schema = uri;
pub type imported_from = String;
pub type see_also = Vec<uriorcurie>;
pub type owned_by = uriorcurie;
pub type created_by = uriorcurie;
pub type contributors = Vec<uriorcurie>;
pub type created_on = NaiveDateTime;
pub type last_updated_on = NaiveDateTime;
pub type modified_by = uriorcurie;
pub type status = uriorcurie;
pub type literal_form = String;
pub type alias_predicate = AliasPredicateEnum;
pub type alias_contexts = Vec<uri>;
pub type in_language = String;
pub type source = uriorcurie;
pub type publisher = uriorcurie;
pub type is_a = Definition;
pub type abstract_ = bool;
pub type mixin = bool;
pub type mixins = Vec<Definition>;
pub type apply_to = Vec<Definition>;
pub type values_from = Vec<uriorcurie>;
pub type code_set = uriorcurie;
pub type code_set_version = String;
pub type code_set_tag = String;
pub type pv_formula = PvFormulaOptions;
pub type permissible_values = Vec<PermissibleValue>;
pub type enum_uri = uriorcurie;
pub type include = Vec<AnonymousEnumExpression>;
pub type minus = Vec<AnonymousEnumExpression>;
pub type inherits = Vec<EnumDefinition>;
pub type matches = MatchQuery;
pub type identifier_pattern = String;
pub type concepts = Vec<uriorcurie>;
pub type reachable_from = ReachabilityQuery;
pub type source_ontology = uriorcurie;
pub type is_direct = bool;
pub type traverse_up = bool;
pub type include_self = bool;
pub type relationship_types = Vec<uriorcurie>;
pub type source_nodes = Vec<uriorcurie>;
pub type text = String;
pub type meaning = uriorcurie;
pub type id = uri;
pub type emit_prefixes = Vec<ncname>;
pub type version = String;
pub type imports = Vec<uriorcurie>;
pub type structured_imports = Vec<ImportExpression>;
pub type license = String;
pub type default_curi_maps = Vec<String>;
pub type default_prefix = String;
pub type default_range = TypeDefinition;
pub type subsets = Vec<SubsetDefinition>;
pub type types = Vec<TypeDefinition>;
pub type enums = Vec<EnumDefinition>;
pub type slot_definitions = Vec<SlotDefinition>;
pub type classes = Vec<ClassDefinition>;
pub type metamodel_version = String;
pub type source_file = String;
pub type source_file_date = NaiveDateTime;
pub type source_file_size = isize;
pub type generation_date = NaiveDateTime;
pub type slots = Vec<SlotDefinition>;
pub type slot_usage = Vec<SlotDefinition>;
pub type enum_range = EnumExpression;
pub type range_expression = AnonymousClassExpression;
pub type boolean_slot = Vec<Expression>;
pub type any_of = Vec<Expression>;
pub type exactly_one_of = Vec<Expression>;
pub type none_of = Vec<Expression>;
pub type all_of = Vec<Expression>;
pub type preconditions = AnonymousClassExpression;
pub type postconditions = AnonymousClassExpression;
pub type elseconditions = AnonymousClassExpression;
pub type bidirectional = bool;
pub type open_world = bool;
pub type rank = isize;
pub type deactivated = bool;
pub type rules = Vec<ClassRule>;
pub type classification_rules = Vec<AnonymousClassExpression>;
pub type slot_conditions = Vec<SlotDefinition>;
pub type attributes = Vec<SlotDefinition>;
pub type class_uri = uriorcurie;
pub type subclass_of = uriorcurie;
pub type defining_slots = Vec<SlotDefinition>;
pub type union_of = Vec<Element>;
pub type tree_root = bool;
pub type unique_keys = Vec<UniqueKey>;
pub type unique_key_name = String;
pub type consider_nulls_inequal = bool;
pub type unique_key_slots = Vec<SlotDefinition>;
pub type slot_names_unique = bool;
pub type domain = ClassDefinition;
pub type range = Element;
pub type slot_uri = uriorcurie;
pub type multivalued = bool;
pub type array = ArrayExpression;
pub type dimensions = Vec<DimensionExpression>;
pub type minimum_number_dimensions = isize;
pub type maximum_number_dimensions = Anything;
pub type exact_number_dimensions = isize;
pub type inherited = bool;
pub type readonly = String;
pub type ifabsent = String;
pub type implicit_prefix = String;
pub type value_specification_constant = String;
pub type list_value_specification_constant = String;
pub type value_presence = PresenceEnum;
pub type equals_string = String;
pub type equals_number = isize;
pub type equals_expression = String;
pub type exact_cardinality = isize;
pub type minimum_cardinality = isize;
pub type maximum_cardinality = isize;
pub type equals_string_in = Vec<String>;
pub type equals_number_in = Vec<isize>;
pub type has_member = AnonymousSlotExpression;
pub type all_members = AnonymousSlotExpression;
pub type singular_name = String;
pub type required = bool;
pub type recommended = bool;
pub type inapplicable = bool;
pub type inlined = bool;
pub type inlined_as_list = bool;
pub type inlined_as_simple_dict = bool;
pub type list_elements_ordered = bool;
pub type list_elements_unique = bool;
pub type shared = bool;
pub type key = bool;
pub type identifier = bool;
pub type designates_type = bool;
pub type alias = String;
pub type owner = Definition;
pub type domain_of = Vec<ClassDefinition>;
pub type is_usage_slot = bool;
pub type usage_slot_name = String;
pub type subproperty_of = SlotDefinition;
pub type disjoint_with = Vec<Definition>;
pub type children_are_mutually_disjoint = bool;
pub type relational_logical_characteristic = bool;
pub type symmetric = bool;
pub type asymmetric = bool;
pub type reflexive = bool;
pub type irreflexive = bool;
pub type locally_reflexive = bool;
pub type transitive = bool;
pub type transitive_form_of = SlotDefinition;
pub type reflexive_transitive_form_of = SlotDefinition;
pub type inverse = SlotDefinition;
pub type is_class_field = bool;
pub type role = String;
pub type minimum_value = Anything;
pub type maximum_value = Anything;
pub type interpolated = bool;
pub type partial_match = bool;
pub type pattern = String;
pub type syntax = String;
pub type structured_pattern = PatternExpression;
pub type string_serialization = String;
pub type bindings = Vec<EnumBinding>;
pub type binds_value_of = String;
pub type obligation_level = ObligationLevelEnum;
pub type type_mappings = Vec<TypeMapping>;
pub type framework_key = String;
pub type mapped_type = TypeDefinition;
pub type typeof_ = TypeDefinition;
pub type base = String;
pub type type_uri = uriorcurie;
pub type repr = String;
pub type alt_description_text = String;
pub type alt_description_source = String;
pub type alt_descriptions = Vec<AltDescription>;
pub type value = String;
pub type value_description = String;
pub type value_object = Anything;
pub type examples = Vec<Example>;
pub type prefix_prefix = ncname;
pub type prefix_reference = uri;
pub type prefixes = Vec<Prefix>;
pub type setting_key = ncname;
pub type setting_value = String;
pub type settings = Vec<Setting>;
pub type import_from = uriorcurie;
pub type import_as = ncname;
pub type import_map = Vec<Setting>;
pub type local_name_source = ncname;
pub type local_name_value = String;
pub type local_names = Vec<LocalName>;
pub type slot_group = SlotDefinition;
pub type is_grouping_slot = bool;
pub type followed_by = Expression;
pub type reversed = bool;
pub type traverse = SlotDefinition;
pub type path_rule = PathExpression;
pub type represents_relationship = bool;
pub type relational_role = RelationalRoleEnum;

// Enums

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PvFormulaOptions {
    #[cfg_attr(feature = "serde", serde(rename = "CODE"))]
    CODE,
    #[cfg_attr(feature = "serde", serde(rename = "CURIE"))]
    CURIE,
    #[cfg_attr(feature = "serde", serde(rename = "URI"))]
    URI,
    #[cfg_attr(feature = "serde", serde(rename = "FHIR_CODING"))]
    FHIRCODING,
    #[cfg_attr(feature = "serde", serde(rename = "LABEL"))]
    LABEL,
}

impl core::fmt::Display for PvFormulaOptions {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PvFormulaOptions::CODE => f.write_str("CODE"),
            PvFormulaOptions::CURIE => f.write_str("CURIE"),
            PvFormulaOptions::URI => f.write_str("URI"),
            PvFormulaOptions::FHIRCODING => f.write_str("FHIR_CODING"),
            PvFormulaOptions::LABEL => f.write_str("LABEL"),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for PvFormulaOptions {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s: &str = match self {
            PvFormulaOptions::CODE => "CODE",
            PvFormulaOptions::CURIE => "CURIE",
            PvFormulaOptions::URI => "URI",
            PvFormulaOptions::FHIRCODING => "FHIR_CODING",
            PvFormulaOptions::LABEL => "LABEL",
        };
        Ok(pyo3::types::PyString::new(py, s).into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for PvFormulaOptions {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(s) = ob.extract::<&str>() {
            match s {
                "CODE" => Ok(PvFormulaOptions::CODE),
                "CURIE" => Ok(PvFormulaOptions::CURIE),
                "URI" => Ok(PvFormulaOptions::URI),
                "FHIR_CODING" | "FHIRCODING" => Ok(PvFormulaOptions::FHIRCODING),
                "LABEL" => Ok(PvFormulaOptions::LABEL),
                _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "invalid value for PvFormulaOptions: {}",
                    s
                ))),
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(concat!(
                "expected str for ",
                stringify!(PvFormulaOptions)
            )))
        }
    }
}

#[cfg(feature = "stubgen")]
impl ::pyo3_stub_gen::PyStubType for PvFormulaOptions {
    fn type_output() -> ::pyo3_stub_gen::TypeInfo {
        ::pyo3_stub_gen::TypeInfo::with_module(
            "typing.Literal['CODE', 'CURIE', 'URI', 'FHIR_CODING', 'LABEL']",
            "typing".into(),
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PresenceEnum {
    #[cfg_attr(feature = "serde", serde(rename = "UNCOMMITTED"))]
    UNCOMMITTED,
    #[cfg_attr(feature = "serde", serde(rename = "PRESENT"))]
    PRESENT,
    #[cfg_attr(feature = "serde", serde(rename = "ABSENT"))]
    ABSENT,
}

impl core::fmt::Display for PresenceEnum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PresenceEnum::UNCOMMITTED => f.write_str("UNCOMMITTED"),
            PresenceEnum::PRESENT => f.write_str("PRESENT"),
            PresenceEnum::ABSENT => f.write_str("ABSENT"),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for PresenceEnum {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s: &str = match self {
            PresenceEnum::UNCOMMITTED => "UNCOMMITTED",
            PresenceEnum::PRESENT => "PRESENT",
            PresenceEnum::ABSENT => "ABSENT",
        };
        Ok(pyo3::types::PyString::new(py, s).into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for PresenceEnum {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(s) = ob.extract::<&str>() {
            match s {
                "UNCOMMITTED" => Ok(PresenceEnum::UNCOMMITTED),
                "PRESENT" => Ok(PresenceEnum::PRESENT),
                "ABSENT" => Ok(PresenceEnum::ABSENT),
                _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "invalid value for PresenceEnum: {}",
                    s
                ))),
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(concat!(
                "expected str for ",
                stringify!(PresenceEnum)
            )))
        }
    }
}

#[cfg(feature = "stubgen")]
impl ::pyo3_stub_gen::PyStubType for PresenceEnum {
    fn type_output() -> ::pyo3_stub_gen::TypeInfo {
        ::pyo3_stub_gen::TypeInfo::with_module(
            "typing.Literal['UNCOMMITTED', 'PRESENT', 'ABSENT']",
            "typing".into(),
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum RelationalRoleEnum {
    #[cfg_attr(feature = "serde", serde(rename = "SUBJECT"))]
    SUBJECT,
    #[cfg_attr(feature = "serde", serde(rename = "OBJECT"))]
    OBJECT,
    #[cfg_attr(feature = "serde", serde(rename = "PREDICATE"))]
    PREDICATE,
    #[cfg_attr(feature = "serde", serde(rename = "NODE"))]
    NODE,
    #[cfg_attr(feature = "serde", serde(rename = "OTHER_ROLE"))]
    OTHERROLE,
}

impl core::fmt::Display for RelationalRoleEnum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RelationalRoleEnum::SUBJECT => f.write_str("SUBJECT"),
            RelationalRoleEnum::OBJECT => f.write_str("OBJECT"),
            RelationalRoleEnum::PREDICATE => f.write_str("PREDICATE"),
            RelationalRoleEnum::NODE => f.write_str("NODE"),
            RelationalRoleEnum::OTHERROLE => f.write_str("OTHER_ROLE"),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for RelationalRoleEnum {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s: &str = match self {
            RelationalRoleEnum::SUBJECT => "SUBJECT",
            RelationalRoleEnum::OBJECT => "OBJECT",
            RelationalRoleEnum::PREDICATE => "PREDICATE",
            RelationalRoleEnum::NODE => "NODE",
            RelationalRoleEnum::OTHERROLE => "OTHER_ROLE",
        };
        Ok(pyo3::types::PyString::new(py, s).into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for RelationalRoleEnum {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(s) = ob.extract::<&str>() {
            match s {
                "SUBJECT" => Ok(RelationalRoleEnum::SUBJECT),
                "OBJECT" => Ok(RelationalRoleEnum::OBJECT),
                "PREDICATE" => Ok(RelationalRoleEnum::PREDICATE),
                "NODE" => Ok(RelationalRoleEnum::NODE),
                "OTHER_ROLE" | "OTHERROLE" => Ok(RelationalRoleEnum::OTHERROLE),
                _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "invalid value for RelationalRoleEnum: {}",
                    s
                ))),
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(concat!(
                "expected str for ",
                stringify!(RelationalRoleEnum)
            )))
        }
    }
}

#[cfg(feature = "stubgen")]
impl ::pyo3_stub_gen::PyStubType for RelationalRoleEnum {
    fn type_output() -> ::pyo3_stub_gen::TypeInfo {
        ::pyo3_stub_gen::TypeInfo::with_module(
            "typing.Literal['SUBJECT', 'OBJECT', 'PREDICATE', 'NODE', 'OTHER_ROLE']",
            "typing".into(),
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AliasPredicateEnum {
    #[cfg_attr(feature = "serde", serde(rename = "EXACT_SYNONYM"))]
    EXACTSYNONYM,
    #[cfg_attr(feature = "serde", serde(rename = "RELATED_SYNONYM"))]
    RELATEDSYNONYM,
    #[cfg_attr(feature = "serde", serde(rename = "BROAD_SYNONYM"))]
    BROADSYNONYM,
    #[cfg_attr(feature = "serde", serde(rename = "NARROW_SYNONYM"))]
    NARROWSYNONYM,
}

impl core::fmt::Display for AliasPredicateEnum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            AliasPredicateEnum::EXACTSYNONYM => f.write_str("EXACT_SYNONYM"),
            AliasPredicateEnum::RELATEDSYNONYM => f.write_str("RELATED_SYNONYM"),
            AliasPredicateEnum::BROADSYNONYM => f.write_str("BROAD_SYNONYM"),
            AliasPredicateEnum::NARROWSYNONYM => f.write_str("NARROW_SYNONYM"),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for AliasPredicateEnum {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s: &str = match self {
            AliasPredicateEnum::EXACTSYNONYM => "EXACT_SYNONYM",
            AliasPredicateEnum::RELATEDSYNONYM => "RELATED_SYNONYM",
            AliasPredicateEnum::BROADSYNONYM => "BROAD_SYNONYM",
            AliasPredicateEnum::NARROWSYNONYM => "NARROW_SYNONYM",
        };
        Ok(pyo3::types::PyString::new(py, s).into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for AliasPredicateEnum {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(s) = ob.extract::<&str>() {
            match s {
                "EXACT_SYNONYM" | "EXACTSYNONYM" => Ok(AliasPredicateEnum::EXACTSYNONYM),
                "RELATED_SYNONYM" | "RELATEDSYNONYM" => Ok(AliasPredicateEnum::RELATEDSYNONYM),
                "BROAD_SYNONYM" | "BROADSYNONYM" => Ok(AliasPredicateEnum::BROADSYNONYM),
                "NARROW_SYNONYM" | "NARROWSYNONYM" => Ok(AliasPredicateEnum::NARROWSYNONYM),
                _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "invalid value for AliasPredicateEnum: {}",
                    s
                ))),
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(concat!(
                "expected str for ",
                stringify!(AliasPredicateEnum)
            )))
        }
    }
}

#[cfg(feature = "stubgen")]
impl ::pyo3_stub_gen::PyStubType for AliasPredicateEnum {
    fn type_output() -> ::pyo3_stub_gen::TypeInfo {
        ::pyo3_stub_gen::TypeInfo::with_module(
            "typing.Literal['EXACT_SYNONYM', 'RELATED_SYNONYM', 'BROAD_SYNONYM', 'NARROW_SYNONYM']",
            "typing".into(),
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ObligationLevelEnum {
    #[cfg_attr(feature = "serde", serde(rename = "REQUIRED"))]
    REQUIRED,
    #[cfg_attr(feature = "serde", serde(rename = "RECOMMENDED"))]
    RECOMMENDED,
    #[cfg_attr(feature = "serde", serde(rename = "OPTIONAL"))]
    OPTIONAL,
    #[cfg_attr(feature = "serde", serde(rename = "EXAMPLE"))]
    EXAMPLE,
    #[cfg_attr(feature = "serde", serde(rename = "DISCOURAGED"))]
    DISCOURAGED,
}

impl core::fmt::Display for ObligationLevelEnum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ObligationLevelEnum::REQUIRED => f.write_str("REQUIRED"),
            ObligationLevelEnum::RECOMMENDED => f.write_str("RECOMMENDED"),
            ObligationLevelEnum::OPTIONAL => f.write_str("OPTIONAL"),
            ObligationLevelEnum::EXAMPLE => f.write_str("EXAMPLE"),
            ObligationLevelEnum::DISCOURAGED => f.write_str("DISCOURAGED"),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for ObligationLevelEnum {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s: &str = match self {
            ObligationLevelEnum::REQUIRED => "REQUIRED",
            ObligationLevelEnum::RECOMMENDED => "RECOMMENDED",
            ObligationLevelEnum::OPTIONAL => "OPTIONAL",
            ObligationLevelEnum::EXAMPLE => "EXAMPLE",
            ObligationLevelEnum::DISCOURAGED => "DISCOURAGED",
        };
        Ok(pyo3::types::PyString::new(py, s).into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for ObligationLevelEnum {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(s) = ob.extract::<&str>() {
            match s {
                "REQUIRED" => Ok(ObligationLevelEnum::REQUIRED),
                "RECOMMENDED" => Ok(ObligationLevelEnum::RECOMMENDED),
                "OPTIONAL" => Ok(ObligationLevelEnum::OPTIONAL),
                "EXAMPLE" => Ok(ObligationLevelEnum::EXAMPLE),
                "DISCOURAGED" => Ok(ObligationLevelEnum::DISCOURAGED),
                _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "invalid value for ObligationLevelEnum: {}",
                    s
                ))),
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(concat!(
                "expected str for ",
                stringify!(ObligationLevelEnum)
            )))
        }
    }
}

#[cfg(feature = "stubgen")]
impl ::pyo3_stub_gen::PyStubType for ObligationLevelEnum {
    fn type_output() -> ::pyo3_stub_gen::TypeInfo {
        ::pyo3_stub_gen::TypeInfo::with_module(
            "typing.Literal['REQUIRED', 'RECOMMENDED', 'OPTIONAL', 'EXAMPLE', 'DISCOURAGED']",
            "typing".into(),
        )
    }
}

// Classes

pub type AnyValue = Anything;

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Extension {
    #[cfg_attr(feature = "serde", serde(alias = "tag"))]
    pub extension_tag: uriorcurie,
    #[cfg_attr(feature = "serde", serde(alias = "value"))]
    pub extension_value: AnyValue,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, Box<ExtensionOrSubtype>>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Extension {
    #[new]
    #[pyo3(signature = (extension_tag, extension_value, extensions=None))]
    pub fn new(
        extension_tag: uriorcurie,
        extension_value: serde_utils::PyValue<AnyValue>,
        extensions: Option<serde_utils::PyValue<HashMap<String, Box<ExtensionOrSubtype>>>>,
    ) -> Self {
        let extension_value = extension_value.into_inner();
        let extensions = extensions.map(|v| v.into_inner());
        Extension {
            extension_tag,
            extension_value,
            extensions,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Extension> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Extension> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Extension>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Extension",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for Extension {
    type Key = uriorcurie;
    type Value = AnyValue;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.extension_tag;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("extension_tag".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("extension_tag".into()), Value::String(k));
        map.insert(Value::String("extension_value".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum ExtensionOrSubtype {
    Annotation(Annotation),
}

impl From<Annotation> for ExtensionOrSubtype {
    fn from(x: Annotation) -> Self {
        Self::Annotation(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for ExtensionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Annotation>() {
            return Ok(ExtensionOrSubtype::Annotation(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ExtensionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for ExtensionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ExtensionOrSubtype::Annotation(val) => val.into_pyobject(py).map(move |b| b.into_any()),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ExtensionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ExtensionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ExtensionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ExtensionOrSubtype",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for ExtensionOrSubtype {
    type Key = String;
    type Value = serde_value::Value;
    type Error = String;

    fn from_pair_mapping(k: Self::Key, v: Self::Value) -> Result<Self, Self::Error> {
        if let Ok(x) = Annotation::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ExtensionOrSubtype::Annotation(x));
        }
        Err("none of the variants matched the mapping form".into())
    }

    fn from_pair_simple(k: Self::Key, v: Self::Value) -> Result<Self, Self::Error> {
        if let Ok(x) = Annotation::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ExtensionOrSubtype::Annotation(x));
        }
        Err("none of the variants support the primitive form".into())
    }

    fn extract_key(&self) -> &Self::Key {
        match self {
            ExtensionOrSubtype::Annotation(inner) => inner.extract_key(),
        }
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(ExtensionOrSubtype = Annotation);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Extensible {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Extensible {
    #[new]
    #[pyo3(signature = (extensions=None))]
    pub fn new(
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        Extensible { extensions }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Extensible> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Extensible> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Extensible>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Extensible",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum ExtensibleOrSubtype {
    Element(Element),
    EnumBinding(EnumBinding),
    StructuredAlias(StructuredAlias),
    AnonymousExpression(AnonymousExpression),
    PathExpression(PathExpression),
    ClassRule(ClassRule),
    ArrayExpression(ArrayExpression),
    DimensionExpression(DimensionExpression),
    PatternExpression(PatternExpression),
    ImportExpression(ImportExpression),
    PermissibleValue(PermissibleValue),
    UniqueKey(UniqueKey),
    TypeMapping(TypeMapping),
    AnonymousSlotExpression(AnonymousSlotExpression),
    AnonymousClassExpression(AnonymousClassExpression),
    SchemaDefinition(SchemaDefinition),
    TypeDefinition(TypeDefinition),
    SubsetDefinition(SubsetDefinition),
    Definition(Definition),
    EnumDefinition(EnumDefinition),
    SlotDefinition(SlotDefinition),
    ClassDefinition(ClassDefinition),
}

impl From<Element> for ExtensibleOrSubtype {
    fn from(x: Element) -> Self {
        Self::Element(x)
    }
}
impl From<EnumBinding> for ExtensibleOrSubtype {
    fn from(x: EnumBinding) -> Self {
        Self::EnumBinding(x)
    }
}
impl From<StructuredAlias> for ExtensibleOrSubtype {
    fn from(x: StructuredAlias) -> Self {
        Self::StructuredAlias(x)
    }
}
impl From<AnonymousExpression> for ExtensibleOrSubtype {
    fn from(x: AnonymousExpression) -> Self {
        Self::AnonymousExpression(x)
    }
}
impl From<PathExpression> for ExtensibleOrSubtype {
    fn from(x: PathExpression) -> Self {
        Self::PathExpression(x)
    }
}
impl From<ClassRule> for ExtensibleOrSubtype {
    fn from(x: ClassRule) -> Self {
        Self::ClassRule(x)
    }
}
impl From<ArrayExpression> for ExtensibleOrSubtype {
    fn from(x: ArrayExpression) -> Self {
        Self::ArrayExpression(x)
    }
}
impl From<DimensionExpression> for ExtensibleOrSubtype {
    fn from(x: DimensionExpression) -> Self {
        Self::DimensionExpression(x)
    }
}
impl From<PatternExpression> for ExtensibleOrSubtype {
    fn from(x: PatternExpression) -> Self {
        Self::PatternExpression(x)
    }
}
impl From<ImportExpression> for ExtensibleOrSubtype {
    fn from(x: ImportExpression) -> Self {
        Self::ImportExpression(x)
    }
}
impl From<PermissibleValue> for ExtensibleOrSubtype {
    fn from(x: PermissibleValue) -> Self {
        Self::PermissibleValue(x)
    }
}
impl From<UniqueKey> for ExtensibleOrSubtype {
    fn from(x: UniqueKey) -> Self {
        Self::UniqueKey(x)
    }
}
impl From<TypeMapping> for ExtensibleOrSubtype {
    fn from(x: TypeMapping) -> Self {
        Self::TypeMapping(x)
    }
}
impl From<AnonymousSlotExpression> for ExtensibleOrSubtype {
    fn from(x: AnonymousSlotExpression) -> Self {
        Self::AnonymousSlotExpression(x)
    }
}
impl From<AnonymousClassExpression> for ExtensibleOrSubtype {
    fn from(x: AnonymousClassExpression) -> Self {
        Self::AnonymousClassExpression(x)
    }
}
impl From<SchemaDefinition> for ExtensibleOrSubtype {
    fn from(x: SchemaDefinition) -> Self {
        Self::SchemaDefinition(x)
    }
}
impl From<TypeDefinition> for ExtensibleOrSubtype {
    fn from(x: TypeDefinition) -> Self {
        Self::TypeDefinition(x)
    }
}
impl From<SubsetDefinition> for ExtensibleOrSubtype {
    fn from(x: SubsetDefinition) -> Self {
        Self::SubsetDefinition(x)
    }
}
impl From<Definition> for ExtensibleOrSubtype {
    fn from(x: Definition) -> Self {
        Self::Definition(x)
    }
}
impl From<EnumDefinition> for ExtensibleOrSubtype {
    fn from(x: EnumDefinition) -> Self {
        Self::EnumDefinition(x)
    }
}
impl From<SlotDefinition> for ExtensibleOrSubtype {
    fn from(x: SlotDefinition) -> Self {
        Self::SlotDefinition(x)
    }
}
impl From<ClassDefinition> for ExtensibleOrSubtype {
    fn from(x: ClassDefinition) -> Self {
        Self::ClassDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for ExtensibleOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Element>() {
            return Ok(ExtensibleOrSubtype::Element(val));
        }
        if let Ok(val) = ob.extract::<EnumBinding>() {
            return Ok(ExtensibleOrSubtype::EnumBinding(val));
        }
        if let Ok(val) = ob.extract::<StructuredAlias>() {
            return Ok(ExtensibleOrSubtype::StructuredAlias(val));
        }
        if let Ok(val) = ob.extract::<AnonymousExpression>() {
            return Ok(ExtensibleOrSubtype::AnonymousExpression(val));
        }
        if let Ok(val) = ob.extract::<PathExpression>() {
            return Ok(ExtensibleOrSubtype::PathExpression(val));
        }
        if let Ok(val) = ob.extract::<ClassRule>() {
            return Ok(ExtensibleOrSubtype::ClassRule(val));
        }
        if let Ok(val) = ob.extract::<ArrayExpression>() {
            return Ok(ExtensibleOrSubtype::ArrayExpression(val));
        }
        if let Ok(val) = ob.extract::<DimensionExpression>() {
            return Ok(ExtensibleOrSubtype::DimensionExpression(val));
        }
        if let Ok(val) = ob.extract::<PatternExpression>() {
            return Ok(ExtensibleOrSubtype::PatternExpression(val));
        }
        if let Ok(val) = ob.extract::<ImportExpression>() {
            return Ok(ExtensibleOrSubtype::ImportExpression(val));
        }
        if let Ok(val) = ob.extract::<PermissibleValue>() {
            return Ok(ExtensibleOrSubtype::PermissibleValue(val));
        }
        if let Ok(val) = ob.extract::<UniqueKey>() {
            return Ok(ExtensibleOrSubtype::UniqueKey(val));
        }
        if let Ok(val) = ob.extract::<TypeMapping>() {
            return Ok(ExtensibleOrSubtype::TypeMapping(val));
        }
        if let Ok(val) = ob.extract::<AnonymousSlotExpression>() {
            return Ok(ExtensibleOrSubtype::AnonymousSlotExpression(val));
        }
        if let Ok(val) = ob.extract::<AnonymousClassExpression>() {
            return Ok(ExtensibleOrSubtype::AnonymousClassExpression(val));
        }
        if let Ok(val) = ob.extract::<SchemaDefinition>() {
            return Ok(ExtensibleOrSubtype::SchemaDefinition(val));
        }
        if let Ok(val) = ob.extract::<TypeDefinition>() {
            return Ok(ExtensibleOrSubtype::TypeDefinition(val));
        }
        if let Ok(val) = ob.extract::<SubsetDefinition>() {
            return Ok(ExtensibleOrSubtype::SubsetDefinition(val));
        }
        if let Ok(val) = ob.extract::<Definition>() {
            return Ok(ExtensibleOrSubtype::Definition(val));
        }
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(ExtensibleOrSubtype::EnumDefinition(val));
        }
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(ExtensibleOrSubtype::SlotDefinition(val));
        }
        if let Ok(val) = ob.extract::<ClassDefinition>() {
            return Ok(ExtensibleOrSubtype::ClassDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ExtensibleOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for ExtensibleOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ExtensibleOrSubtype::Element(val) => val.into_pyobject(py).map(move |b| b.into_any()),
            ExtensibleOrSubtype::EnumBinding(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::StructuredAlias(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::AnonymousExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::PathExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::ClassRule(val) => val.into_pyobject(py).map(move |b| b.into_any()),
            ExtensibleOrSubtype::ArrayExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::DimensionExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::PatternExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::ImportExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::PermissibleValue(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::UniqueKey(val) => val.into_pyobject(py).map(move |b| b.into_any()),
            ExtensibleOrSubtype::TypeMapping(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::AnonymousSlotExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::AnonymousClassExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::SchemaDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::TypeDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::SubsetDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::Definition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::EnumDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::SlotDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExtensibleOrSubtype::ClassDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ExtensibleOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ExtensibleOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ExtensibleOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ExtensibleOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    ExtensibleOrSubtype = Element
        | EnumBinding
        | StructuredAlias
        | AnonymousExpression
        | PathExpression
        | ClassRule
        | ArrayExpression
        | DimensionExpression
        | PatternExpression
        | ImportExpression
        | PermissibleValue
        | UniqueKey
        | TypeMapping
        | AnonymousSlotExpression
        | AnonymousClassExpression
        | SchemaDefinition
        | TypeDefinition
        | SubsetDefinition
        | Definition
        | EnumDefinition
        | SlotDefinition
        | ClassDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Annotatable {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Annotatable {
    #[new]
    #[pyo3(signature = (annotations=None))]
    pub fn new(annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>) -> Self {
        let annotations = annotations.map(|v| v.into_inner());
        Annotatable { annotations }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Annotatable> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Annotatable> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Annotatable>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Annotatable",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum AnnotatableOrSubtype {
    Annotation(Annotation),
    Element(Element),
    EnumBinding(EnumBinding),
    StructuredAlias(StructuredAlias),
    AnonymousExpression(AnonymousExpression),
    PathExpression(PathExpression),
    ClassRule(ClassRule),
    ArrayExpression(ArrayExpression),
    DimensionExpression(DimensionExpression),
    PatternExpression(PatternExpression),
    ImportExpression(ImportExpression),
    PermissibleValue(PermissibleValue),
    UniqueKey(UniqueKey),
    TypeMapping(TypeMapping),
    AnonymousSlotExpression(AnonymousSlotExpression),
    AnonymousClassExpression(AnonymousClassExpression),
    SchemaDefinition(SchemaDefinition),
    TypeDefinition(TypeDefinition),
    SubsetDefinition(SubsetDefinition),
    Definition(Definition),
    EnumDefinition(EnumDefinition),
    SlotDefinition(SlotDefinition),
    ClassDefinition(ClassDefinition),
}

impl From<Annotation> for AnnotatableOrSubtype {
    fn from(x: Annotation) -> Self {
        Self::Annotation(x)
    }
}
impl From<Element> for AnnotatableOrSubtype {
    fn from(x: Element) -> Self {
        Self::Element(x)
    }
}
impl From<EnumBinding> for AnnotatableOrSubtype {
    fn from(x: EnumBinding) -> Self {
        Self::EnumBinding(x)
    }
}
impl From<StructuredAlias> for AnnotatableOrSubtype {
    fn from(x: StructuredAlias) -> Self {
        Self::StructuredAlias(x)
    }
}
impl From<AnonymousExpression> for AnnotatableOrSubtype {
    fn from(x: AnonymousExpression) -> Self {
        Self::AnonymousExpression(x)
    }
}
impl From<PathExpression> for AnnotatableOrSubtype {
    fn from(x: PathExpression) -> Self {
        Self::PathExpression(x)
    }
}
impl From<ClassRule> for AnnotatableOrSubtype {
    fn from(x: ClassRule) -> Self {
        Self::ClassRule(x)
    }
}
impl From<ArrayExpression> for AnnotatableOrSubtype {
    fn from(x: ArrayExpression) -> Self {
        Self::ArrayExpression(x)
    }
}
impl From<DimensionExpression> for AnnotatableOrSubtype {
    fn from(x: DimensionExpression) -> Self {
        Self::DimensionExpression(x)
    }
}
impl From<PatternExpression> for AnnotatableOrSubtype {
    fn from(x: PatternExpression) -> Self {
        Self::PatternExpression(x)
    }
}
impl From<ImportExpression> for AnnotatableOrSubtype {
    fn from(x: ImportExpression) -> Self {
        Self::ImportExpression(x)
    }
}
impl From<PermissibleValue> for AnnotatableOrSubtype {
    fn from(x: PermissibleValue) -> Self {
        Self::PermissibleValue(x)
    }
}
impl From<UniqueKey> for AnnotatableOrSubtype {
    fn from(x: UniqueKey) -> Self {
        Self::UniqueKey(x)
    }
}
impl From<TypeMapping> for AnnotatableOrSubtype {
    fn from(x: TypeMapping) -> Self {
        Self::TypeMapping(x)
    }
}
impl From<AnonymousSlotExpression> for AnnotatableOrSubtype {
    fn from(x: AnonymousSlotExpression) -> Self {
        Self::AnonymousSlotExpression(x)
    }
}
impl From<AnonymousClassExpression> for AnnotatableOrSubtype {
    fn from(x: AnonymousClassExpression) -> Self {
        Self::AnonymousClassExpression(x)
    }
}
impl From<SchemaDefinition> for AnnotatableOrSubtype {
    fn from(x: SchemaDefinition) -> Self {
        Self::SchemaDefinition(x)
    }
}
impl From<TypeDefinition> for AnnotatableOrSubtype {
    fn from(x: TypeDefinition) -> Self {
        Self::TypeDefinition(x)
    }
}
impl From<SubsetDefinition> for AnnotatableOrSubtype {
    fn from(x: SubsetDefinition) -> Self {
        Self::SubsetDefinition(x)
    }
}
impl From<Definition> for AnnotatableOrSubtype {
    fn from(x: Definition) -> Self {
        Self::Definition(x)
    }
}
impl From<EnumDefinition> for AnnotatableOrSubtype {
    fn from(x: EnumDefinition) -> Self {
        Self::EnumDefinition(x)
    }
}
impl From<SlotDefinition> for AnnotatableOrSubtype {
    fn from(x: SlotDefinition) -> Self {
        Self::SlotDefinition(x)
    }
}
impl From<ClassDefinition> for AnnotatableOrSubtype {
    fn from(x: ClassDefinition) -> Self {
        Self::ClassDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for AnnotatableOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Annotation>() {
            return Ok(AnnotatableOrSubtype::Annotation(val));
        }
        if let Ok(val) = ob.extract::<Element>() {
            return Ok(AnnotatableOrSubtype::Element(val));
        }
        if let Ok(val) = ob.extract::<EnumBinding>() {
            return Ok(AnnotatableOrSubtype::EnumBinding(val));
        }
        if let Ok(val) = ob.extract::<StructuredAlias>() {
            return Ok(AnnotatableOrSubtype::StructuredAlias(val));
        }
        if let Ok(val) = ob.extract::<AnonymousExpression>() {
            return Ok(AnnotatableOrSubtype::AnonymousExpression(val));
        }
        if let Ok(val) = ob.extract::<PathExpression>() {
            return Ok(AnnotatableOrSubtype::PathExpression(val));
        }
        if let Ok(val) = ob.extract::<ClassRule>() {
            return Ok(AnnotatableOrSubtype::ClassRule(val));
        }
        if let Ok(val) = ob.extract::<ArrayExpression>() {
            return Ok(AnnotatableOrSubtype::ArrayExpression(val));
        }
        if let Ok(val) = ob.extract::<DimensionExpression>() {
            return Ok(AnnotatableOrSubtype::DimensionExpression(val));
        }
        if let Ok(val) = ob.extract::<PatternExpression>() {
            return Ok(AnnotatableOrSubtype::PatternExpression(val));
        }
        if let Ok(val) = ob.extract::<ImportExpression>() {
            return Ok(AnnotatableOrSubtype::ImportExpression(val));
        }
        if let Ok(val) = ob.extract::<PermissibleValue>() {
            return Ok(AnnotatableOrSubtype::PermissibleValue(val));
        }
        if let Ok(val) = ob.extract::<UniqueKey>() {
            return Ok(AnnotatableOrSubtype::UniqueKey(val));
        }
        if let Ok(val) = ob.extract::<TypeMapping>() {
            return Ok(AnnotatableOrSubtype::TypeMapping(val));
        }
        if let Ok(val) = ob.extract::<AnonymousSlotExpression>() {
            return Ok(AnnotatableOrSubtype::AnonymousSlotExpression(val));
        }
        if let Ok(val) = ob.extract::<AnonymousClassExpression>() {
            return Ok(AnnotatableOrSubtype::AnonymousClassExpression(val));
        }
        if let Ok(val) = ob.extract::<SchemaDefinition>() {
            return Ok(AnnotatableOrSubtype::SchemaDefinition(val));
        }
        if let Ok(val) = ob.extract::<TypeDefinition>() {
            return Ok(AnnotatableOrSubtype::TypeDefinition(val));
        }
        if let Ok(val) = ob.extract::<SubsetDefinition>() {
            return Ok(AnnotatableOrSubtype::SubsetDefinition(val));
        }
        if let Ok(val) = ob.extract::<Definition>() {
            return Ok(AnnotatableOrSubtype::Definition(val));
        }
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(AnnotatableOrSubtype::EnumDefinition(val));
        }
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(AnnotatableOrSubtype::SlotDefinition(val));
        }
        if let Ok(val) = ob.extract::<ClassDefinition>() {
            return Ok(AnnotatableOrSubtype::ClassDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnnotatableOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for AnnotatableOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            AnnotatableOrSubtype::Annotation(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::Element(val) => val.into_pyobject(py).map(move |b| b.into_any()),
            AnnotatableOrSubtype::EnumBinding(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::StructuredAlias(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::AnonymousExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::PathExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::ClassRule(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::ArrayExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::DimensionExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::PatternExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::ImportExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::PermissibleValue(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::UniqueKey(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::TypeMapping(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::AnonymousSlotExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::AnonymousClassExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::SchemaDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::TypeDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::SubsetDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::Definition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::EnumDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::SlotDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnnotatableOrSubtype::ClassDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AnnotatableOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AnnotatableOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnnotatableOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnnotatableOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    AnnotatableOrSubtype = Annotation
        | Element
        | EnumBinding
        | StructuredAlias
        | AnonymousExpression
        | PathExpression
        | ClassRule
        | ArrayExpression
        | DimensionExpression
        | PatternExpression
        | ImportExpression
        | PermissibleValue
        | UniqueKey
        | TypeMapping
        | AnonymousSlotExpression
        | AnonymousClassExpression
        | SchemaDefinition
        | TypeDefinition
        | SubsetDefinition
        | Definition
        | EnumDefinition
        | SlotDefinition
        | ClassDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Annotation {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Box<Annotation>>>,
    #[cfg_attr(feature = "serde", serde(alias = "tag"))]
    pub extension_tag: uriorcurie,
    #[cfg_attr(feature = "serde", serde(alias = "value"))]
    pub extension_value: AnyValue,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Annotation {
    #[new]
    #[pyo3(signature = (extension_tag, extension_value, annotations=None, extensions=None))]
    pub fn new(
        extension_tag: uriorcurie,
        extension_value: serde_utils::PyValue<AnyValue>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Box<Annotation>>>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
    ) -> Self {
        let extension_value = extension_value.into_inner();
        let annotations = annotations.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        Annotation {
            extension_tag,
            extension_value,
            annotations,
            extensions,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Annotation> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Annotation> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Annotation>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Annotation",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for Annotation {
    type Key = uriorcurie;
    type Value = AnyValue;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.extension_tag;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("extension_tag".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("extension_tag".into()), Value::String(k));
        map.insert(Value::String("extension_value".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct UnitOfMeasure {
    #[cfg_attr(feature = "serde", serde(default))]
    pub symbol: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub abbreviation: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub descriptive_name: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub ucum_code: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub derivation: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub has_quantity_kind: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub iec61360code: Option<String>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl UnitOfMeasure {
    #[new]
    #[pyo3(signature = (symbol=None, abbreviation=None, descriptive_name=None, exact_mappings=None, ucum_code=None, derivation=None, has_quantity_kind=None, iec61360code=None))]
    pub fn new(
        symbol: Option<String>,
        abbreviation: Option<String>,
        descriptive_name: Option<String>,
        exact_mappings: Option<Vec<uriorcurie>>,
        ucum_code: Option<String>,
        derivation: Option<String>,
        has_quantity_kind: Option<uriorcurie>,
        iec61360code: Option<String>,
    ) -> Self {
        UnitOfMeasure {
            symbol,
            abbreviation,
            descriptive_name,
            exact_mappings,
            ucum_code,
            derivation,
            has_quantity_kind,
            iec61360code,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<UnitOfMeasure> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<UnitOfMeasure> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<UnitOfMeasure>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid UnitOfMeasure",
        ))
    }
}

#[derive(Clone, PartialEq)]
pub struct Anything(
    #[cfg(feature = "serde")] pub serde_value::Value,
    #[cfg(not(feature = "serde"))] pub (),
);

#[cfg(feature = "stubgen")]
impl ::pyo3_stub_gen::PyStubType for Anything {
    fn type_output() -> ::pyo3_stub_gen::TypeInfo {
        ::pyo3_stub_gen::TypeInfo::any()
    }
}

#[cfg(feature = "serde")]
impl Serialize for Anything {
    fn serialize<S>(&self, to_ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(to_ser)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for Anything {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <serde_value::Value as Deserialize>::deserialize(de).map(Anything)
    }
}

#[cfg(all(feature = "pyo3", feature = "serde"))]
impl<'py> FromPyObject<'py> for Anything {
    fn extract_bound(obj: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::{PyAny, PyDict, PyList, PyTuple};
        use serde_value::Value;

        fn py_to_value<'py>(o: &pyo3::Bound<'py, PyAny>) -> pyo3::PyResult<Value> {
            // None -> Unit
            if o.is_none() {
                return Ok(Value::Unit);
            }

            // Try simple primitives first
            if let Ok(s) = o.extract::<&str>() {
                return Ok(Value::String(s.to_string()));
            }
            if let Ok(b) = o.extract::<bool>() {
                return Ok(Value::Bool(b));
            }

            // Sequences (list/tuple)
            if let Ok(list) = o.downcast::<PyList>() {
                let mut out = Vec::with_capacity(list.len());
                for item in list.iter() {
                    out.push(py_to_value(&item)?);
                }
                return Ok(Value::Seq(out));
            }
            if let Ok(t) = o.downcast::<PyTuple>() {
                let mut out = Vec::with_capacity(t.len());
                for item in t.iter() {
                    out.push(py_to_value(&item)?);
                }
                return Ok(Value::Seq(out));
            }

            // Mappings (dict with string-like keys)
            if let Ok(d) = o.downcast::<PyDict>() {
                let mut map = std::collections::BTreeMap::<Value, Value>::new();
                for (k, v) in d.iter() {
                    // Only accept string-like keys for deterministic ordering
                    if let Ok(ks) = k.extract::<&str>() {
                        map.insert(Value::String(ks.to_string()), py_to_value(&v)?);
                    } else {
                        return Err(pyo3::exceptions::PyTypeError::new_err(
                            "dict keys for Anything must be str",
                        ));
                    }
                }
                return Ok(Value::Map(map));
            }

            // Fallback: stringify unknown types
            let s = format!("{}", o.str()?);
            Ok(Value::String(s))
        }

        Ok(Anything(py_to_value(obj)?))
    }
}

/* ---------- getter side ---------- */
#[cfg(all(feature = "pyo3", feature = "serde"))]
impl<'py> IntoPyObject<'py> for Anything {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        use pyo3::types::{PyAny, PyDict, PyList, PyString};
        use serde_value::Value;

        fn value_to_py<'py>(py: Python<'py>, v: &Value) -> pyo3::PyResult<Bound<'py, PyAny>> {
            match v {
                Value::Unit => Ok(py.None().into_bound(py)),
                Value::Bool(b) => Ok(pyo3::types::PyBool::new(py, *b).to_owned().into_any()),
                Value::String(s) => Ok(PyString::new(py, s).into_any()),
                Value::Seq(seq) => {
                    let list = PyList::empty(py);
                    for item in seq.iter() {
                        let ob = value_to_py(py, item)?;
                        list.append(ob)?;
                    }
                    Ok(list.into_any())
                }
                Value::Map(map) => {
                    let dict = PyDict::new(py);
                    for (k, v) in map.iter() {
                        let pk = value_to_py(py, k)?;
                        let pv = value_to_py(py, v)?;
                        dict.set_item(pk, pv)?;
                    }
                    Ok(dict.into_any())
                }
                // Best-effort for other serde_value variants
                // (numbers, bytes, chars, etc.)
                other => {
                    // Try common cases without bringing extra deps
                    // Numbers are converted via string if not covered above
                    let s = format!("{:?}", other);
                    Ok(PyString::new(py, &s).into_any())
                }
            }
        }

        value_to_py(py, &self.0)
    }
}

impl std::fmt::Debug for Anything {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "serde")]
        return write!(f, "Anything({:?})", self.0);

        #[cfg(not(feature = "serde"))]
        return f.write_str("Anything(<opaque>)");
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct CommonMetadata {
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl CommonMetadata {
    #[new]
    #[pyo3(signature = (description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        CommonMetadata {
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<CommonMetadata> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<CommonMetadata> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<CommonMetadata>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid CommonMetadata",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum CommonMetadataOrSubtype {
    Element(Element),
    EnumBinding(EnumBinding),
    StructuredAlias(StructuredAlias),
    AnonymousExpression(AnonymousExpression),
    PathExpression(PathExpression),
    ClassRule(ClassRule),
    ArrayExpression(ArrayExpression),
    DimensionExpression(DimensionExpression),
    PatternExpression(PatternExpression),
    ImportExpression(ImportExpression),
    PermissibleValue(PermissibleValue),
    UniqueKey(UniqueKey),
    TypeMapping(TypeMapping),
    AnonymousSlotExpression(AnonymousSlotExpression),
    AnonymousClassExpression(AnonymousClassExpression),
    SchemaDefinition(SchemaDefinition),
    TypeDefinition(TypeDefinition),
    SubsetDefinition(SubsetDefinition),
    Definition(Definition),
    EnumDefinition(EnumDefinition),
    SlotDefinition(SlotDefinition),
    ClassDefinition(ClassDefinition),
}

impl From<Element> for CommonMetadataOrSubtype {
    fn from(x: Element) -> Self {
        Self::Element(x)
    }
}
impl From<EnumBinding> for CommonMetadataOrSubtype {
    fn from(x: EnumBinding) -> Self {
        Self::EnumBinding(x)
    }
}
impl From<StructuredAlias> for CommonMetadataOrSubtype {
    fn from(x: StructuredAlias) -> Self {
        Self::StructuredAlias(x)
    }
}
impl From<AnonymousExpression> for CommonMetadataOrSubtype {
    fn from(x: AnonymousExpression) -> Self {
        Self::AnonymousExpression(x)
    }
}
impl From<PathExpression> for CommonMetadataOrSubtype {
    fn from(x: PathExpression) -> Self {
        Self::PathExpression(x)
    }
}
impl From<ClassRule> for CommonMetadataOrSubtype {
    fn from(x: ClassRule) -> Self {
        Self::ClassRule(x)
    }
}
impl From<ArrayExpression> for CommonMetadataOrSubtype {
    fn from(x: ArrayExpression) -> Self {
        Self::ArrayExpression(x)
    }
}
impl From<DimensionExpression> for CommonMetadataOrSubtype {
    fn from(x: DimensionExpression) -> Self {
        Self::DimensionExpression(x)
    }
}
impl From<PatternExpression> for CommonMetadataOrSubtype {
    fn from(x: PatternExpression) -> Self {
        Self::PatternExpression(x)
    }
}
impl From<ImportExpression> for CommonMetadataOrSubtype {
    fn from(x: ImportExpression) -> Self {
        Self::ImportExpression(x)
    }
}
impl From<PermissibleValue> for CommonMetadataOrSubtype {
    fn from(x: PermissibleValue) -> Self {
        Self::PermissibleValue(x)
    }
}
impl From<UniqueKey> for CommonMetadataOrSubtype {
    fn from(x: UniqueKey) -> Self {
        Self::UniqueKey(x)
    }
}
impl From<TypeMapping> for CommonMetadataOrSubtype {
    fn from(x: TypeMapping) -> Self {
        Self::TypeMapping(x)
    }
}
impl From<AnonymousSlotExpression> for CommonMetadataOrSubtype {
    fn from(x: AnonymousSlotExpression) -> Self {
        Self::AnonymousSlotExpression(x)
    }
}
impl From<AnonymousClassExpression> for CommonMetadataOrSubtype {
    fn from(x: AnonymousClassExpression) -> Self {
        Self::AnonymousClassExpression(x)
    }
}
impl From<SchemaDefinition> for CommonMetadataOrSubtype {
    fn from(x: SchemaDefinition) -> Self {
        Self::SchemaDefinition(x)
    }
}
impl From<TypeDefinition> for CommonMetadataOrSubtype {
    fn from(x: TypeDefinition) -> Self {
        Self::TypeDefinition(x)
    }
}
impl From<SubsetDefinition> for CommonMetadataOrSubtype {
    fn from(x: SubsetDefinition) -> Self {
        Self::SubsetDefinition(x)
    }
}
impl From<Definition> for CommonMetadataOrSubtype {
    fn from(x: Definition) -> Self {
        Self::Definition(x)
    }
}
impl From<EnumDefinition> for CommonMetadataOrSubtype {
    fn from(x: EnumDefinition) -> Self {
        Self::EnumDefinition(x)
    }
}
impl From<SlotDefinition> for CommonMetadataOrSubtype {
    fn from(x: SlotDefinition) -> Self {
        Self::SlotDefinition(x)
    }
}
impl From<ClassDefinition> for CommonMetadataOrSubtype {
    fn from(x: ClassDefinition) -> Self {
        Self::ClassDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for CommonMetadataOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Element>() {
            return Ok(CommonMetadataOrSubtype::Element(val));
        }
        if let Ok(val) = ob.extract::<EnumBinding>() {
            return Ok(CommonMetadataOrSubtype::EnumBinding(val));
        }
        if let Ok(val) = ob.extract::<StructuredAlias>() {
            return Ok(CommonMetadataOrSubtype::StructuredAlias(val));
        }
        if let Ok(val) = ob.extract::<AnonymousExpression>() {
            return Ok(CommonMetadataOrSubtype::AnonymousExpression(val));
        }
        if let Ok(val) = ob.extract::<PathExpression>() {
            return Ok(CommonMetadataOrSubtype::PathExpression(val));
        }
        if let Ok(val) = ob.extract::<ClassRule>() {
            return Ok(CommonMetadataOrSubtype::ClassRule(val));
        }
        if let Ok(val) = ob.extract::<ArrayExpression>() {
            return Ok(CommonMetadataOrSubtype::ArrayExpression(val));
        }
        if let Ok(val) = ob.extract::<DimensionExpression>() {
            return Ok(CommonMetadataOrSubtype::DimensionExpression(val));
        }
        if let Ok(val) = ob.extract::<PatternExpression>() {
            return Ok(CommonMetadataOrSubtype::PatternExpression(val));
        }
        if let Ok(val) = ob.extract::<ImportExpression>() {
            return Ok(CommonMetadataOrSubtype::ImportExpression(val));
        }
        if let Ok(val) = ob.extract::<PermissibleValue>() {
            return Ok(CommonMetadataOrSubtype::PermissibleValue(val));
        }
        if let Ok(val) = ob.extract::<UniqueKey>() {
            return Ok(CommonMetadataOrSubtype::UniqueKey(val));
        }
        if let Ok(val) = ob.extract::<TypeMapping>() {
            return Ok(CommonMetadataOrSubtype::TypeMapping(val));
        }
        if let Ok(val) = ob.extract::<AnonymousSlotExpression>() {
            return Ok(CommonMetadataOrSubtype::AnonymousSlotExpression(val));
        }
        if let Ok(val) = ob.extract::<AnonymousClassExpression>() {
            return Ok(CommonMetadataOrSubtype::AnonymousClassExpression(val));
        }
        if let Ok(val) = ob.extract::<SchemaDefinition>() {
            return Ok(CommonMetadataOrSubtype::SchemaDefinition(val));
        }
        if let Ok(val) = ob.extract::<TypeDefinition>() {
            return Ok(CommonMetadataOrSubtype::TypeDefinition(val));
        }
        if let Ok(val) = ob.extract::<SubsetDefinition>() {
            return Ok(CommonMetadataOrSubtype::SubsetDefinition(val));
        }
        if let Ok(val) = ob.extract::<Definition>() {
            return Ok(CommonMetadataOrSubtype::Definition(val));
        }
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(CommonMetadataOrSubtype::EnumDefinition(val));
        }
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(CommonMetadataOrSubtype::SlotDefinition(val));
        }
        if let Ok(val) = ob.extract::<ClassDefinition>() {
            return Ok(CommonMetadataOrSubtype::ClassDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid CommonMetadataOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for CommonMetadataOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            CommonMetadataOrSubtype::Element(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::EnumBinding(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::TypeMapping(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::Definition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<CommonMetadataOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<CommonMetadataOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<CommonMetadataOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid CommonMetadataOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    CommonMetadataOrSubtype = Element
        | EnumBinding
        | StructuredAlias
        | AnonymousExpression
        | PathExpression
        | ClassRule
        | ArrayExpression
        | DimensionExpression
        | PatternExpression
        | ImportExpression
        | PermissibleValue
        | UniqueKey
        | TypeMapping
        | AnonymousSlotExpression
        | AnonymousClassExpression
        | SchemaDefinition
        | TypeDefinition
        | SubsetDefinition
        | Definition
        | EnumDefinition
        | SlotDefinition
        | ClassDefinition
);

pub mod element_utl {
    use super::*;
    #[derive(Debug, Clone, PartialEq)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum name_range {
        String(String),
        ncname(ncname),
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for name_range {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<String>() {
                return Ok(name_range::String(val));
            }
            if let Ok(val) = ob.extract::<ncname>() {
                return Ok(name_range::ncname(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid name",
            ))
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for name_range {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                name_range::String(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                name_range::ncname(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for Box<name_range> {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;
        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (*self).into_pyobject(py).map(move |x| x.into_any())
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for Box<name_range> {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<name_range>() {
                return Ok(Box::new(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid name",
            ))
        }
    }

    #[cfg(feature = "stubgen")]
    ::pyo3_stub_gen::impl_stub_type!(name_range = String | ncname);
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Element {
    pub name: String,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Element {
    #[new]
    #[pyo3(signature = (name, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        name: String,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        Element {
            name,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Element> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Element> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Element>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Element",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for Element {
    type Key = String;
    type Value = bool;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("id_prefixes_are_closed".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum ElementOrSubtype {
    SchemaDefinition(SchemaDefinition),
    TypeDefinition(TypeDefinition),
    SubsetDefinition(SubsetDefinition),
    Definition(Definition),
    EnumDefinition(EnumDefinition),
    SlotDefinition(SlotDefinition),
    ClassDefinition(ClassDefinition),
}

impl From<SchemaDefinition> for ElementOrSubtype {
    fn from(x: SchemaDefinition) -> Self {
        Self::SchemaDefinition(x)
    }
}
impl From<TypeDefinition> for ElementOrSubtype {
    fn from(x: TypeDefinition) -> Self {
        Self::TypeDefinition(x)
    }
}
impl From<SubsetDefinition> for ElementOrSubtype {
    fn from(x: SubsetDefinition) -> Self {
        Self::SubsetDefinition(x)
    }
}
impl From<Definition> for ElementOrSubtype {
    fn from(x: Definition) -> Self {
        Self::Definition(x)
    }
}
impl From<EnumDefinition> for ElementOrSubtype {
    fn from(x: EnumDefinition) -> Self {
        Self::EnumDefinition(x)
    }
}
impl From<SlotDefinition> for ElementOrSubtype {
    fn from(x: SlotDefinition) -> Self {
        Self::SlotDefinition(x)
    }
}
impl From<ClassDefinition> for ElementOrSubtype {
    fn from(x: ClassDefinition) -> Self {
        Self::ClassDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for ElementOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<SchemaDefinition>() {
            return Ok(ElementOrSubtype::SchemaDefinition(val));
        }
        if let Ok(val) = ob.extract::<TypeDefinition>() {
            return Ok(ElementOrSubtype::TypeDefinition(val));
        }
        if let Ok(val) = ob.extract::<SubsetDefinition>() {
            return Ok(ElementOrSubtype::SubsetDefinition(val));
        }
        if let Ok(val) = ob.extract::<Definition>() {
            return Ok(ElementOrSubtype::Definition(val));
        }
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(ElementOrSubtype::EnumDefinition(val));
        }
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(ElementOrSubtype::SlotDefinition(val));
        }
        if let Ok(val) = ob.extract::<ClassDefinition>() {
            return Ok(ElementOrSubtype::ClassDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ElementOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for ElementOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ElementOrSubtype::TypeDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ElementOrSubtype::SubsetDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ElementOrSubtype::Definition(val) => val.into_pyobject(py).map(move |b| b.into_any()),
            ElementOrSubtype::EnumDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ElementOrSubtype::SlotDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ElementOrSubtype::ClassDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ElementOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ElementOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ElementOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ElementOrSubtype",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for ElementOrSubtype {
    type Key = String;
    type Value = serde_value::Value;
    type Error = String;

    fn from_pair_mapping(k: Self::Key, v: Self::Value) -> Result<Self, Self::Error> {
        if let Ok(x) = SchemaDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::SchemaDefinition(x));
        }
        if let Ok(x) = TypeDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::TypeDefinition(x));
        }
        if let Ok(x) = SubsetDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::SubsetDefinition(x));
        }
        if let Ok(x) = Definition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::Definition(x));
        }
        if let Ok(x) = EnumDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::EnumDefinition(x));
        }
        if let Ok(x) = SlotDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::SlotDefinition(x));
        }
        if let Ok(x) = ClassDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::ClassDefinition(x));
        }
        Err("none of the variants matched the mapping form".into())
    }

    fn from_pair_simple(k: Self::Key, v: Self::Value) -> Result<Self, Self::Error> {
        if let Ok(x) = SchemaDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::SchemaDefinition(x));
        }
        if let Ok(x) = TypeDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::TypeDefinition(x));
        }
        if let Ok(x) = SubsetDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::SubsetDefinition(x));
        }
        if let Ok(x) = Definition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::Definition(x));
        }
        if let Ok(x) = EnumDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::EnumDefinition(x));
        }
        if let Ok(x) = SlotDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::SlotDefinition(x));
        }
        if let Ok(x) = ClassDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(ElementOrSubtype::ClassDefinition(x));
        }
        Err("none of the variants support the primitive form".into())
    }

    fn extract_key(&self) -> &Self::Key {
        match self {
            ElementOrSubtype::SchemaDefinition(inner) => inner.extract_key(),
            ElementOrSubtype::TypeDefinition(inner) => inner.extract_key(),
            ElementOrSubtype::SubsetDefinition(inner) => inner.extract_key(),
            ElementOrSubtype::Definition(inner) => inner.extract_key(),
            ElementOrSubtype::EnumDefinition(inner) => inner.extract_key(),
            ElementOrSubtype::SlotDefinition(inner) => inner.extract_key(),
            ElementOrSubtype::ClassDefinition(inner) => inner.extract_key(),
        }
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    ElementOrSubtype = SchemaDefinition
        | TypeDefinition
        | SubsetDefinition
        | Definition
        | EnumDefinition
        | SlotDefinition
        | ClassDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct SchemaDefinition {
    pub id: uri,
    #[cfg_attr(feature = "serde", serde(default))]
    pub version: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub imports: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub license: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub prefixes: Option<HashMap<String, Prefix>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub emit_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub default_curi_maps: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub default_prefix: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub default_range: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub subsets: Option<HashMap<String, SubsetDefinition>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub types: Option<HashMap<String, TypeDefinition>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub enums: Option<HashMap<String, EnumDefinition>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "slots"))]
    pub slot_definitions: Option<HashMap<String, SlotDefinition>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub classes: Option<HashMap<String, ClassDefinition>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub metamodel_version: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_file: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_file_date: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_file_size: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub generation_date: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_names_unique: Option<bool>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub settings: Option<HashMap<String, Setting>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub bindings: Option<Vec<EnumBinding>>,
    pub name: ncname,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl SchemaDefinition {
    #[new]
    #[pyo3(signature = (id, name, version=None, imports=None, license=None, prefixes=None, emit_prefixes=None, default_curi_maps=None, default_prefix=None, default_range=None, subsets=None, types=None, enums=None, slot_definitions=None, classes=None, metamodel_version=None, source_file=None, source_file_date=None, source_file_size=None, generation_date=None, slot_names_unique=None, settings=None, bindings=None, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        id: uri,
        name: ncname,
        version: Option<String>,
        imports: Option<Vec<uriorcurie>>,
        license: Option<String>,
        prefixes: Option<serde_utils::PyValue<HashMap<String, Prefix>>>,
        emit_prefixes: Option<Vec<ncname>>,
        default_curi_maps: Option<Vec<String>>,
        default_prefix: Option<String>,
        default_range: Option<String>,
        subsets: Option<serde_utils::PyValue<HashMap<String, SubsetDefinition>>>,
        types: Option<serde_utils::PyValue<HashMap<String, TypeDefinition>>>,
        enums: Option<serde_utils::PyValue<HashMap<String, EnumDefinition>>>,
        slot_definitions: Option<serde_utils::PyValue<HashMap<String, SlotDefinition>>>,
        classes: Option<serde_utils::PyValue<HashMap<String, ClassDefinition>>>,
        metamodel_version: Option<String>,
        source_file: Option<String>,
        source_file_date: Option<NaiveDateTime>,
        source_file_size: Option<isize>,
        generation_date: Option<NaiveDateTime>,
        slot_names_unique: Option<bool>,
        settings: Option<serde_utils::PyValue<HashMap<String, Setting>>>,
        bindings: Option<serde_utils::PyValue<Vec<EnumBinding>>>,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let prefixes = prefixes.map(|v| v.into_inner());
        let subsets = subsets.map(|v| v.into_inner());
        let types = types.map(|v| v.into_inner());
        let enums = enums.map(|v| v.into_inner());
        let slot_definitions = slot_definitions.map(|v| v.into_inner());
        let classes = classes.map(|v| v.into_inner());
        let settings = settings.map(|v| v.into_inner());
        let bindings = bindings.map(|v| v.into_inner());
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        SchemaDefinition {
            id,
            name,
            version,
            imports,
            license,
            prefixes,
            emit_prefixes,
            default_curi_maps,
            default_prefix,
            default_range,
            subsets,
            types,
            enums,
            slot_definitions,
            classes,
            metamodel_version,
            source_file,
            source_file_date,
            source_file_size,
            generation_date,
            slot_names_unique,
            settings,
            bindings,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<SchemaDefinition> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<SchemaDefinition> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<SchemaDefinition>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid SchemaDefinition",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for SchemaDefinition {
    type Key = ncname;
    type Value = uri;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("id".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct AnonymousTypeExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub pattern: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_pattern: Option<PatternExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub unit: Option<UnitOfMeasure>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub implicit_prefix: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string_in: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_number: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<Box<AnonymousTypeExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<Box<AnonymousTypeExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<Box<AnonymousTypeExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<Box<AnonymousTypeExpression>>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl AnonymousTypeExpression {
    #[new]
    #[pyo3(signature = (pattern=None, structured_pattern=None, unit=None, implicit_prefix=None, equals_string=None, equals_string_in=None, equals_number=None, minimum_value=None, maximum_value=None, none_of=None, exactly_one_of=None, any_of=None, all_of=None))]
    pub fn new(
        pattern: Option<String>,
        structured_pattern: Option<serde_utils::PyValue<PatternExpression>>,
        unit: Option<serde_utils::PyValue<UnitOfMeasure>>,
        implicit_prefix: Option<String>,
        equals_string: Option<String>,
        equals_string_in: Option<Vec<String>>,
        equals_number: Option<isize>,
        minimum_value: Option<serde_utils::PyValue<Anything>>,
        maximum_value: Option<serde_utils::PyValue<Anything>>,
        none_of: Option<serde_utils::PyValue<Vec<Box<AnonymousTypeExpression>>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<Box<AnonymousTypeExpression>>>>,
        any_of: Option<serde_utils::PyValue<Vec<Box<AnonymousTypeExpression>>>>,
        all_of: Option<serde_utils::PyValue<Vec<Box<AnonymousTypeExpression>>>>,
    ) -> Self {
        let structured_pattern = structured_pattern.map(|v| v.into_inner());
        let unit = unit.map(|v| v.into_inner());
        let minimum_value = minimum_value.map(|v| v.into_inner());
        let maximum_value = maximum_value.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        AnonymousTypeExpression {
            pattern,
            structured_pattern,
            unit,
            implicit_prefix,
            equals_string,
            equals_string_in,
            equals_number,
            minimum_value,
            maximum_value,
            none_of,
            exactly_one_of,
            any_of,
            all_of,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AnonymousTypeExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AnonymousTypeExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousTypeExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnonymousTypeExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct TypeDefinition {
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "typeof"))]
    pub typeof_: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub base: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "uri"))]
    pub type_uri: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub repr: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub union_of: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub pattern: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_pattern: Option<PatternExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub unit: Option<UnitOfMeasure>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub implicit_prefix: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string_in: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_number: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<AnonymousTypeExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<AnonymousTypeExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<AnonymousTypeExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<AnonymousTypeExpression>>,
    pub name: String,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl TypeDefinition {
    #[new]
    #[pyo3(signature = (name, typeof_=None, base=None, type_uri=None, repr=None, union_of=None, pattern=None, structured_pattern=None, unit=None, implicit_prefix=None, equals_string=None, equals_string_in=None, equals_number=None, minimum_value=None, maximum_value=None, none_of=None, exactly_one_of=None, any_of=None, all_of=None, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        name: String,
        typeof_: Option<String>,
        base: Option<String>,
        type_uri: Option<uriorcurie>,
        repr: Option<String>,
        union_of: Option<Vec<String>>,
        pattern: Option<String>,
        structured_pattern: Option<serde_utils::PyValue<PatternExpression>>,
        unit: Option<serde_utils::PyValue<UnitOfMeasure>>,
        implicit_prefix: Option<String>,
        equals_string: Option<String>,
        equals_string_in: Option<Vec<String>>,
        equals_number: Option<isize>,
        minimum_value: Option<serde_utils::PyValue<Anything>>,
        maximum_value: Option<serde_utils::PyValue<Anything>>,
        none_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
        any_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
        all_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let structured_pattern = structured_pattern.map(|v| v.into_inner());
        let unit = unit.map(|v| v.into_inner());
        let minimum_value = minimum_value.map(|v| v.into_inner());
        let maximum_value = maximum_value.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        TypeDefinition {
            name,
            typeof_,
            base,
            type_uri,
            repr,
            union_of,
            pattern,
            structured_pattern,
            unit,
            implicit_prefix,
            equals_string,
            equals_string_in,
            equals_number,
            minimum_value,
            maximum_value,
            none_of,
            exactly_one_of,
            any_of,
            all_of,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<TypeDefinition> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<TypeDefinition> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<TypeDefinition>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid TypeDefinition",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for TypeDefinition {
    type Key = String;
    type Value = TypeDefinition;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("typeof_".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct SubsetDefinition {
    pub name: String,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<Box<StructuredAlias>>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl SubsetDefinition {
    #[new]
    #[pyo3(signature = (name, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        name: String,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<Box<StructuredAlias>>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        SubsetDefinition {
            name,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<SubsetDefinition> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<SubsetDefinition> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<SubsetDefinition>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid SubsetDefinition",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for SubsetDefinition {
    type Key = String;
    type Value = bool;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("id_prefixes_are_closed".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

pub mod definition_utl {
    use super::*;
    #[derive(Debug, Clone, PartialEq)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum is_a_range {
        Definition(Definition),
        SlotDefinition(SlotDefinition),
        ClassDefinition(ClassDefinition),
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for is_a_range {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<Definition>() {
                return Ok(is_a_range::Definition(val));
            }
            if let Ok(val) = ob.extract::<SlotDefinition>() {
                return Ok(is_a_range::SlotDefinition(val));
            }
            if let Ok(val) = ob.extract::<ClassDefinition>() {
                return Ok(is_a_range::ClassDefinition(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid is_a",
            ))
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for is_a_range {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                is_a_range::Definition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                is_a_range::SlotDefinition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                is_a_range::ClassDefinition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for Box<is_a_range> {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;
        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (*self).into_pyobject(py).map(move |x| x.into_any())
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for Box<is_a_range> {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<is_a_range>() {
                return Ok(Box::new(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid is_a",
            ))
        }
    }

    #[cfg(feature = "stubgen")]
    ::pyo3_stub_gen::impl_stub_type!(is_a_range = Definition | SlotDefinition | ClassDefinition);
    #[derive(Debug, Clone, PartialEq)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum mixins_range {
        Definition(Definition),
        SlotDefinition(SlotDefinition),
        ClassDefinition(ClassDefinition),
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for mixins_range {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<Definition>() {
                return Ok(mixins_range::Definition(val));
            }
            if let Ok(val) = ob.extract::<SlotDefinition>() {
                return Ok(mixins_range::SlotDefinition(val));
            }
            if let Ok(val) = ob.extract::<ClassDefinition>() {
                return Ok(mixins_range::ClassDefinition(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid mixins",
            ))
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for mixins_range {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                mixins_range::Definition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                mixins_range::SlotDefinition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                mixins_range::ClassDefinition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for Box<mixins_range> {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;
        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (*self).into_pyobject(py).map(move |x| x.into_any())
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for Box<mixins_range> {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<mixins_range>() {
                return Ok(Box::new(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid mixins",
            ))
        }
    }

    #[cfg(feature = "stubgen")]
    ::pyo3_stub_gen::impl_stub_type!(mixins_range = Definition | SlotDefinition | ClassDefinition);
    #[derive(Debug, Clone, PartialEq)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum apply_to_range {
        Definition(Definition),
        SlotDefinition(SlotDefinition),
        ClassDefinition(ClassDefinition),
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for apply_to_range {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<Definition>() {
                return Ok(apply_to_range::Definition(val));
            }
            if let Ok(val) = ob.extract::<SlotDefinition>() {
                return Ok(apply_to_range::SlotDefinition(val));
            }
            if let Ok(val) = ob.extract::<ClassDefinition>() {
                return Ok(apply_to_range::ClassDefinition(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid apply_to",
            ))
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for apply_to_range {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                apply_to_range::Definition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                apply_to_range::SlotDefinition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                apply_to_range::ClassDefinition(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for Box<apply_to_range> {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;
        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (*self).into_pyobject(py).map(move |x| x.into_any())
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for Box<apply_to_range> {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<apply_to_range>() {
                return Ok(Box::new(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid apply_to",
            ))
        }
    }

    #[cfg(feature = "stubgen")]
    ::pyo3_stub_gen::impl_stub_type!(
        apply_to_range = Definition | SlotDefinition | ClassDefinition
    );
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Definition {
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_a: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "abstract"))]
    pub abstract_: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixin: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixins: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub apply_to: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub values_from: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub string_serialization: Option<String>,
    pub name: String,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Definition {
    #[new]
    #[pyo3(signature = (name, is_a=None, abstract_=None, mixin=None, mixins=None, apply_to=None, values_from=None, string_serialization=None, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        name: String,
        is_a: Option<String>,
        abstract_: Option<bool>,
        mixin: Option<bool>,
        mixins: Option<Vec<String>>,
        apply_to: Option<Vec<String>>,
        values_from: Option<Vec<uriorcurie>>,
        string_serialization: Option<String>,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        Definition {
            name,
            is_a,
            abstract_,
            mixin,
            mixins,
            apply_to,
            values_from,
            string_serialization,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Definition> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Definition> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Definition>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Definition",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for Definition {
    type Key = String;
    type Value = Definition;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("is_a".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum DefinitionOrSubtype {
    EnumDefinition(EnumDefinition),
    SlotDefinition(SlotDefinition),
    ClassDefinition(ClassDefinition),
}

impl From<EnumDefinition> for DefinitionOrSubtype {
    fn from(x: EnumDefinition) -> Self {
        Self::EnumDefinition(x)
    }
}
impl From<SlotDefinition> for DefinitionOrSubtype {
    fn from(x: SlotDefinition) -> Self {
        Self::SlotDefinition(x)
    }
}
impl From<ClassDefinition> for DefinitionOrSubtype {
    fn from(x: ClassDefinition) -> Self {
        Self::ClassDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for DefinitionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(DefinitionOrSubtype::EnumDefinition(val));
        }
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(DefinitionOrSubtype::SlotDefinition(val));
        }
        if let Ok(val) = ob.extract::<ClassDefinition>() {
            return Ok(DefinitionOrSubtype::ClassDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid DefinitionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for DefinitionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            DefinitionOrSubtype::SlotDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            DefinitionOrSubtype::ClassDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<DefinitionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<DefinitionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<DefinitionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid DefinitionOrSubtype",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for DefinitionOrSubtype {
    type Key = String;
    type Value = serde_value::Value;
    type Error = String;

    fn from_pair_mapping(k: Self::Key, v: Self::Value) -> Result<Self, Self::Error> {
        if let Ok(x) = EnumDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(DefinitionOrSubtype::EnumDefinition(x));
        }
        if let Ok(x) = SlotDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(DefinitionOrSubtype::SlotDefinition(x));
        }
        if let Ok(x) = ClassDefinition::from_pair_mapping(k.clone(), v.clone()) {
            return Ok(DefinitionOrSubtype::ClassDefinition(x));
        }
        Err("none of the variants matched the mapping form".into())
    }

    fn from_pair_simple(k: Self::Key, v: Self::Value) -> Result<Self, Self::Error> {
        if let Ok(x) = EnumDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(DefinitionOrSubtype::EnumDefinition(x));
        }
        if let Ok(x) = SlotDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(DefinitionOrSubtype::SlotDefinition(x));
        }
        if let Ok(x) = ClassDefinition::from_pair_simple(k.clone(), v.clone()) {
            return Ok(DefinitionOrSubtype::ClassDefinition(x));
        }
        Err("none of the variants support the primitive form".into())
    }

    fn extract_key(&self) -> &Self::Key {
        match self {
            DefinitionOrSubtype::EnumDefinition(inner) => inner.extract_key(),
            DefinitionOrSubtype::SlotDefinition(inner) => inner.extract_key(),
            DefinitionOrSubtype::ClassDefinition(inner) => inner.extract_key(),
        }
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    DefinitionOrSubtype = EnumDefinition | SlotDefinition | ClassDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct AnonymousEnumExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set_tag: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set_version: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub pv_formula: Option<PvFormulaOptions>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub permissible_values: Option<HashMap<String, PermissibleValue>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub include: Option<Vec<Box<AnonymousEnumExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minus: Option<Vec<Box<AnonymousEnumExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub inherits: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub reachable_from: Option<ReachabilityQuery>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub matches: Option<MatchQuery>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub concepts: Option<Vec<uriorcurie>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl AnonymousEnumExpression {
    #[new]
    #[pyo3(signature = (code_set=None, code_set_tag=None, code_set_version=None, pv_formula=None, permissible_values=None, include=None, minus=None, inherits=None, reachable_from=None, matches=None, concepts=None))]
    pub fn new(
        code_set: Option<uriorcurie>,
        code_set_tag: Option<String>,
        code_set_version: Option<String>,
        pv_formula: Option<PvFormulaOptions>,
        permissible_values: Option<serde_utils::PyValue<HashMap<String, PermissibleValue>>>,
        include: Option<serde_utils::PyValue<Vec<Box<AnonymousEnumExpression>>>>,
        minus: Option<serde_utils::PyValue<Vec<Box<AnonymousEnumExpression>>>>,
        inherits: Option<Vec<String>>,
        reachable_from: Option<serde_utils::PyValue<ReachabilityQuery>>,
        matches: Option<serde_utils::PyValue<MatchQuery>>,
        concepts: Option<Vec<uriorcurie>>,
    ) -> Self {
        let permissible_values = permissible_values.map(|v| v.into_inner());
        let include = include.map(|v| v.into_inner());
        let minus = minus.map(|v| v.into_inner());
        let reachable_from = reachable_from.map(|v| v.into_inner());
        let matches = matches.map(|v| v.into_inner());
        AnonymousEnumExpression {
            code_set,
            code_set_tag,
            code_set_version,
            pv_formula,
            permissible_values,
            include,
            minus,
            inherits,
            reachable_from,
            matches,
            concepts,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AnonymousEnumExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AnonymousEnumExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousEnumExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnonymousEnumExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct EnumDefinition {
    #[cfg_attr(feature = "serde", serde(default))]
    pub enum_uri: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set_tag: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set_version: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub pv_formula: Option<PvFormulaOptions>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub permissible_values: Option<HashMap<String, PermissibleValue>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub include: Option<Vec<Box<AnonymousEnumExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minus: Option<Vec<Box<AnonymousEnumExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub inherits: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub reachable_from: Option<ReachabilityQuery>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub matches: Option<MatchQuery>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub concepts: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_a: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "abstract"))]
    pub abstract_: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixin: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixins: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub apply_to: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub values_from: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub string_serialization: Option<String>,
    pub name: String,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl EnumDefinition {
    #[new]
    #[pyo3(signature = (name, enum_uri=None, code_set=None, code_set_tag=None, code_set_version=None, pv_formula=None, permissible_values=None, include=None, minus=None, inherits=None, reachable_from=None, matches=None, concepts=None, is_a=None, abstract_=None, mixin=None, mixins=None, apply_to=None, values_from=None, string_serialization=None, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        name: String,
        enum_uri: Option<uriorcurie>,
        code_set: Option<uriorcurie>,
        code_set_tag: Option<String>,
        code_set_version: Option<String>,
        pv_formula: Option<PvFormulaOptions>,
        permissible_values: Option<serde_utils::PyValue<HashMap<String, PermissibleValue>>>,
        include: Option<serde_utils::PyValue<Vec<Box<AnonymousEnumExpression>>>>,
        minus: Option<serde_utils::PyValue<Vec<Box<AnonymousEnumExpression>>>>,
        inherits: Option<Vec<String>>,
        reachable_from: Option<serde_utils::PyValue<ReachabilityQuery>>,
        matches: Option<serde_utils::PyValue<MatchQuery>>,
        concepts: Option<Vec<uriorcurie>>,
        is_a: Option<String>,
        abstract_: Option<bool>,
        mixin: Option<bool>,
        mixins: Option<Vec<String>>,
        apply_to: Option<Vec<String>>,
        values_from: Option<Vec<uriorcurie>>,
        string_serialization: Option<String>,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let permissible_values = permissible_values.map(|v| v.into_inner());
        let include = include.map(|v| v.into_inner());
        let minus = minus.map(|v| v.into_inner());
        let reachable_from = reachable_from.map(|v| v.into_inner());
        let matches = matches.map(|v| v.into_inner());
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        EnumDefinition {
            name,
            enum_uri,
            code_set,
            code_set_tag,
            code_set_version,
            pv_formula,
            permissible_values,
            include,
            minus,
            inherits,
            reachable_from,
            matches,
            concepts,
            is_a,
            abstract_,
            mixin,
            mixins,
            apply_to,
            values_from,
            string_serialization,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<EnumDefinition> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<EnumDefinition> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid EnumDefinition",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for EnumDefinition {
    type Key = String;
    type Value = uriorcurie;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("enum_uri".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct EnumBinding {
    #[cfg_attr(feature = "serde", serde(default))]
    pub range: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub obligation_level: Option<ObligationLevelEnum>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub binds_value_of: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub pv_formula: Option<PvFormulaOptions>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl EnumBinding {
    #[new]
    #[pyo3(signature = (range=None, obligation_level=None, binds_value_of=None, pv_formula=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        range: Option<String>,
        obligation_level: Option<ObligationLevelEnum>,
        binds_value_of: Option<String>,
        pv_formula: Option<PvFormulaOptions>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        EnumBinding {
            range,
            obligation_level,
            binds_value_of,
            pv_formula,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<EnumBinding> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<EnumBinding> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<EnumBinding>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid EnumBinding",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct MatchQuery {
    #[cfg_attr(feature = "serde", serde(default))]
    pub identifier_pattern: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_ontology: Option<uriorcurie>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl MatchQuery {
    #[new]
    #[pyo3(signature = (identifier_pattern=None, source_ontology=None))]
    pub fn new(identifier_pattern: Option<String>, source_ontology: Option<uriorcurie>) -> Self {
        MatchQuery {
            identifier_pattern,
            source_ontology,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<MatchQuery> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<MatchQuery> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<MatchQuery>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid MatchQuery",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct ReachabilityQuery {
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_ontology: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_nodes: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub relationship_types: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_direct: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub include_self: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub traverse_up: Option<bool>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl ReachabilityQuery {
    #[new]
    #[pyo3(signature = (source_ontology=None, source_nodes=None, relationship_types=None, is_direct=None, include_self=None, traverse_up=None))]
    pub fn new(
        source_ontology: Option<uriorcurie>,
        source_nodes: Option<Vec<uriorcurie>>,
        relationship_types: Option<Vec<uriorcurie>>,
        is_direct: Option<bool>,
        include_self: Option<bool>,
        traverse_up: Option<bool>,
    ) -> Self {
        ReachabilityQuery {
            source_ontology,
            source_nodes,
            relationship_types,
            is_direct,
            include_self,
            traverse_up,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ReachabilityQuery> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ReachabilityQuery> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ReachabilityQuery>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ReachabilityQuery",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct StructuredAlias {
    pub literal_form: String,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "predicate"))]
    pub alias_predicate: Option<AliasPredicateEnum>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "contexts"))]
    pub alias_contexts: Option<Vec<uri>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<Box<StructuredAlias>>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl StructuredAlias {
    #[new]
    #[pyo3(signature = (literal_form, alias_predicate=None, categories=None, alias_contexts=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, keywords=None))]
    pub fn new(
        literal_form: String,
        alias_predicate: Option<AliasPredicateEnum>,
        categories: Option<Vec<uriorcurie>>,
        alias_contexts: Option<Vec<uri>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<Box<StructuredAlias>>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        StructuredAlias {
            literal_form,
            alias_predicate,
            categories,
            alias_contexts,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<StructuredAlias> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<StructuredAlias> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<StructuredAlias>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid StructuredAlias",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Expression {}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum ExpressionOrSubtype {
    TypeExpression(TypeExpression),
    EnumExpression(EnumExpression),
    StructuredAlias(StructuredAlias),
    AnonymousExpression(AnonymousExpression),
    PathExpression(PathExpression),
    SlotExpression(SlotExpression),
    AnonymousSlotExpression(AnonymousSlotExpression),
    SlotDefinition(SlotDefinition),
    AnonymousClassExpression(AnonymousClassExpression),
    AnonymousEnumExpression(AnonymousEnumExpression),
    EnumDefinition(EnumDefinition),
    AnonymousTypeExpression(AnonymousTypeExpression),
    TypeDefinition(TypeDefinition),
}

impl From<TypeExpression> for ExpressionOrSubtype {
    fn from(x: TypeExpression) -> Self {
        Self::TypeExpression(x)
    }
}
impl From<EnumExpression> for ExpressionOrSubtype {
    fn from(x: EnumExpression) -> Self {
        Self::EnumExpression(x)
    }
}
impl From<StructuredAlias> for ExpressionOrSubtype {
    fn from(x: StructuredAlias) -> Self {
        Self::StructuredAlias(x)
    }
}
impl From<AnonymousExpression> for ExpressionOrSubtype {
    fn from(x: AnonymousExpression) -> Self {
        Self::AnonymousExpression(x)
    }
}
impl From<PathExpression> for ExpressionOrSubtype {
    fn from(x: PathExpression) -> Self {
        Self::PathExpression(x)
    }
}
impl From<SlotExpression> for ExpressionOrSubtype {
    fn from(x: SlotExpression) -> Self {
        Self::SlotExpression(x)
    }
}
impl From<AnonymousSlotExpression> for ExpressionOrSubtype {
    fn from(x: AnonymousSlotExpression) -> Self {
        Self::AnonymousSlotExpression(x)
    }
}
impl From<SlotDefinition> for ExpressionOrSubtype {
    fn from(x: SlotDefinition) -> Self {
        Self::SlotDefinition(x)
    }
}
impl From<AnonymousClassExpression> for ExpressionOrSubtype {
    fn from(x: AnonymousClassExpression) -> Self {
        Self::AnonymousClassExpression(x)
    }
}
impl From<AnonymousEnumExpression> for ExpressionOrSubtype {
    fn from(x: AnonymousEnumExpression) -> Self {
        Self::AnonymousEnumExpression(x)
    }
}
impl From<EnumDefinition> for ExpressionOrSubtype {
    fn from(x: EnumDefinition) -> Self {
        Self::EnumDefinition(x)
    }
}
impl From<AnonymousTypeExpression> for ExpressionOrSubtype {
    fn from(x: AnonymousTypeExpression) -> Self {
        Self::AnonymousTypeExpression(x)
    }
}
impl From<TypeDefinition> for ExpressionOrSubtype {
    fn from(x: TypeDefinition) -> Self {
        Self::TypeDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for ExpressionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<TypeExpression>() {
            return Ok(ExpressionOrSubtype::TypeExpression(val));
        }
        if let Ok(val) = ob.extract::<EnumExpression>() {
            return Ok(ExpressionOrSubtype::EnumExpression(val));
        }
        if let Ok(val) = ob.extract::<StructuredAlias>() {
            return Ok(ExpressionOrSubtype::StructuredAlias(val));
        }
        if let Ok(val) = ob.extract::<AnonymousExpression>() {
            return Ok(ExpressionOrSubtype::AnonymousExpression(val));
        }
        if let Ok(val) = ob.extract::<PathExpression>() {
            return Ok(ExpressionOrSubtype::PathExpression(val));
        }
        if let Ok(val) = ob.extract::<SlotExpression>() {
            return Ok(ExpressionOrSubtype::SlotExpression(val));
        }
        if let Ok(val) = ob.extract::<AnonymousSlotExpression>() {
            return Ok(ExpressionOrSubtype::AnonymousSlotExpression(val));
        }
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(ExpressionOrSubtype::SlotDefinition(val));
        }
        if let Ok(val) = ob.extract::<AnonymousClassExpression>() {
            return Ok(ExpressionOrSubtype::AnonymousClassExpression(val));
        }
        if let Ok(val) = ob.extract::<AnonymousEnumExpression>() {
            return Ok(ExpressionOrSubtype::AnonymousEnumExpression(val));
        }
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(ExpressionOrSubtype::EnumDefinition(val));
        }
        if let Ok(val) = ob.extract::<AnonymousTypeExpression>() {
            return Ok(ExpressionOrSubtype::AnonymousTypeExpression(val));
        }
        if let Ok(val) = ob.extract::<TypeDefinition>() {
            return Ok(ExpressionOrSubtype::TypeDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for ExpressionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ExpressionOrSubtype::TypeExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::EnumExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::StructuredAlias(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::AnonymousExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::PathExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::SlotExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::SlotDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::AnonymousEnumExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::EnumDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::AnonymousTypeExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ExpressionOrSubtype::TypeDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ExpressionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ExpressionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ExpressionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    ExpressionOrSubtype = TypeExpression
        | EnumExpression
        | StructuredAlias
        | AnonymousExpression
        | PathExpression
        | SlotExpression
        | AnonymousSlotExpression
        | SlotDefinition
        | AnonymousClassExpression
        | AnonymousEnumExpression
        | EnumDefinition
        | AnonymousTypeExpression
        | TypeDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct TypeExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub pattern: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_pattern: Option<PatternExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub unit: Option<UnitOfMeasure>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub implicit_prefix: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string_in: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_number: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<AnonymousTypeExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<AnonymousTypeExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<AnonymousTypeExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<AnonymousTypeExpression>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl TypeExpression {
    #[new]
    #[pyo3(signature = (pattern=None, structured_pattern=None, unit=None, implicit_prefix=None, equals_string=None, equals_string_in=None, equals_number=None, minimum_value=None, maximum_value=None, none_of=None, exactly_one_of=None, any_of=None, all_of=None))]
    pub fn new(
        pattern: Option<String>,
        structured_pattern: Option<serde_utils::PyValue<PatternExpression>>,
        unit: Option<serde_utils::PyValue<UnitOfMeasure>>,
        implicit_prefix: Option<String>,
        equals_string: Option<String>,
        equals_string_in: Option<Vec<String>>,
        equals_number: Option<isize>,
        minimum_value: Option<serde_utils::PyValue<Anything>>,
        maximum_value: Option<serde_utils::PyValue<Anything>>,
        none_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
        any_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
        all_of: Option<serde_utils::PyValue<Vec<AnonymousTypeExpression>>>,
    ) -> Self {
        let structured_pattern = structured_pattern.map(|v| v.into_inner());
        let unit = unit.map(|v| v.into_inner());
        let minimum_value = minimum_value.map(|v| v.into_inner());
        let maximum_value = maximum_value.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        TypeExpression {
            pattern,
            structured_pattern,
            unit,
            implicit_prefix,
            equals_string,
            equals_string_in,
            equals_number,
            minimum_value,
            maximum_value,
            none_of,
            exactly_one_of,
            any_of,
            all_of,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<TypeExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<TypeExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<TypeExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid TypeExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum TypeExpressionOrSubtype {
    AnonymousTypeExpression(AnonymousTypeExpression),
    TypeDefinition(TypeDefinition),
}

impl From<AnonymousTypeExpression> for TypeExpressionOrSubtype {
    fn from(x: AnonymousTypeExpression) -> Self {
        Self::AnonymousTypeExpression(x)
    }
}
impl From<TypeDefinition> for TypeExpressionOrSubtype {
    fn from(x: TypeDefinition) -> Self {
        Self::TypeDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for TypeExpressionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousTypeExpression>() {
            return Ok(TypeExpressionOrSubtype::AnonymousTypeExpression(val));
        }
        if let Ok(val) = ob.extract::<TypeDefinition>() {
            return Ok(TypeExpressionOrSubtype::TypeDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid TypeExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for TypeExpressionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            TypeExpressionOrSubtype::TypeDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<TypeExpressionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<TypeExpressionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<TypeExpressionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid TypeExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    TypeExpressionOrSubtype = AnonymousTypeExpression | TypeDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct EnumExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set_tag: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub code_set_version: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub pv_formula: Option<PvFormulaOptions>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub permissible_values: Option<HashMap<String, PermissibleValue>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub include: Option<Vec<AnonymousEnumExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minus: Option<Vec<AnonymousEnumExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub inherits: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub reachable_from: Option<ReachabilityQuery>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub matches: Option<MatchQuery>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub concepts: Option<Vec<uriorcurie>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl EnumExpression {
    #[new]
    #[pyo3(signature = (code_set=None, code_set_tag=None, code_set_version=None, pv_formula=None, permissible_values=None, include=None, minus=None, inherits=None, reachable_from=None, matches=None, concepts=None))]
    pub fn new(
        code_set: Option<uriorcurie>,
        code_set_tag: Option<String>,
        code_set_version: Option<String>,
        pv_formula: Option<PvFormulaOptions>,
        permissible_values: Option<serde_utils::PyValue<HashMap<String, PermissibleValue>>>,
        include: Option<serde_utils::PyValue<Vec<AnonymousEnumExpression>>>,
        minus: Option<serde_utils::PyValue<Vec<AnonymousEnumExpression>>>,
        inherits: Option<Vec<String>>,
        reachable_from: Option<serde_utils::PyValue<ReachabilityQuery>>,
        matches: Option<serde_utils::PyValue<MatchQuery>>,
        concepts: Option<Vec<uriorcurie>>,
    ) -> Self {
        let permissible_values = permissible_values.map(|v| v.into_inner());
        let include = include.map(|v| v.into_inner());
        let minus = minus.map(|v| v.into_inner());
        let reachable_from = reachable_from.map(|v| v.into_inner());
        let matches = matches.map(|v| v.into_inner());
        EnumExpression {
            code_set,
            code_set_tag,
            code_set_version,
            pv_formula,
            permissible_values,
            include,
            minus,
            inherits,
            reachable_from,
            matches,
            concepts,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<EnumExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<EnumExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<EnumExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid EnumExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum EnumExpressionOrSubtype {
    AnonymousEnumExpression(AnonymousEnumExpression),
    EnumDefinition(EnumDefinition),
}

impl From<AnonymousEnumExpression> for EnumExpressionOrSubtype {
    fn from(x: AnonymousEnumExpression) -> Self {
        Self::AnonymousEnumExpression(x)
    }
}
impl From<EnumDefinition> for EnumExpressionOrSubtype {
    fn from(x: EnumDefinition) -> Self {
        Self::EnumDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for EnumExpressionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousEnumExpression>() {
            return Ok(EnumExpressionOrSubtype::AnonymousEnumExpression(val));
        }
        if let Ok(val) = ob.extract::<EnumDefinition>() {
            return Ok(EnumExpressionOrSubtype::EnumDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid EnumExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for EnumExpressionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            EnumExpressionOrSubtype::EnumDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<EnumExpressionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<EnumExpressionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<EnumExpressionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid EnumExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    EnumExpressionOrSubtype = AnonymousEnumExpression | EnumDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct AnonymousExpression {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl AnonymousExpression {
    #[new]
    #[pyo3(signature = (extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        AnonymousExpression {
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AnonymousExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AnonymousExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnonymousExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum AnonymousExpressionOrSubtype {
    AnonymousSlotExpression(AnonymousSlotExpression),
    AnonymousClassExpression(AnonymousClassExpression),
}

impl From<AnonymousSlotExpression> for AnonymousExpressionOrSubtype {
    fn from(x: AnonymousSlotExpression) -> Self {
        Self::AnonymousSlotExpression(x)
    }
}
impl From<AnonymousClassExpression> for AnonymousExpressionOrSubtype {
    fn from(x: AnonymousClassExpression) -> Self {
        Self::AnonymousClassExpression(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for AnonymousExpressionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousSlotExpression>() {
            return Ok(AnonymousExpressionOrSubtype::AnonymousSlotExpression(val));
        }
        if let Ok(val) = ob.extract::<AnonymousClassExpression>() {
            return Ok(AnonymousExpressionOrSubtype::AnonymousClassExpression(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnonymousExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for AnonymousExpressionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AnonymousExpressionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AnonymousExpressionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousExpressionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnonymousExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    AnonymousExpressionOrSubtype = AnonymousSlotExpression | AnonymousClassExpression
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct PathExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub followed_by: Option<Box<PathExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<Box<PathExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<Box<PathExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<Box<PathExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<Box<PathExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub reversed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub traverse: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub range_expression: Option<Box<AnonymousClassExpression>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PathExpression {
    #[new]
    #[pyo3(signature = (followed_by=None, none_of=None, any_of=None, all_of=None, exactly_one_of=None, reversed=None, traverse=None, range_expression=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        followed_by: Option<serde_utils::PyValue<Box<PathExpression>>>,
        none_of: Option<serde_utils::PyValue<Vec<Box<PathExpression>>>>,
        any_of: Option<serde_utils::PyValue<Vec<Box<PathExpression>>>>,
        all_of: Option<serde_utils::PyValue<Vec<Box<PathExpression>>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<Box<PathExpression>>>>,
        reversed: Option<bool>,
        traverse: Option<String>,
        range_expression: Option<serde_utils::PyValue<Box<AnonymousClassExpression>>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let followed_by = followed_by.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let range_expression = range_expression.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        PathExpression {
            followed_by,
            none_of,
            any_of,
            all_of,
            exactly_one_of,
            reversed,
            traverse,
            range_expression,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<PathExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<PathExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<PathExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid PathExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct SlotExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub range: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub range_expression: Option<AnonymousClassExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub enum_range: Option<EnumExpressionOrSubtype>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub bindings: Option<Vec<EnumBinding>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub required: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub recommended: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub multivalued: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub inlined: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub inlined_as_list: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub pattern: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_pattern: Option<PatternExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub unit: Option<UnitOfMeasure>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub implicit_prefix: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub value_presence: Option<PresenceEnum>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string_in: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_number: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_expression: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub has_member: Option<AnonymousSlotExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_members: Option<AnonymousSlotExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<AnonymousSlotExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<AnonymousSlotExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<AnonymousSlotExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<AnonymousSlotExpression>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl SlotExpression {
    #[new]
    #[pyo3(signature = (range=None, range_expression=None, enum_range=None, bindings=None, required=None, recommended=None, multivalued=None, inlined=None, inlined_as_list=None, minimum_value=None, maximum_value=None, pattern=None, structured_pattern=None, unit=None, implicit_prefix=None, value_presence=None, equals_string=None, equals_string_in=None, equals_number=None, equals_expression=None, exact_cardinality=None, minimum_cardinality=None, maximum_cardinality=None, has_member=None, all_members=None, none_of=None, exactly_one_of=None, any_of=None, all_of=None))]
    pub fn new(
        range: Option<String>,
        range_expression: Option<serde_utils::PyValue<AnonymousClassExpression>>,
        enum_range: Option<serde_utils::PyValue<EnumExpressionOrSubtype>>,
        bindings: Option<serde_utils::PyValue<Vec<EnumBinding>>>,
        required: Option<bool>,
        recommended: Option<bool>,
        multivalued: Option<bool>,
        inlined: Option<bool>,
        inlined_as_list: Option<bool>,
        minimum_value: Option<serde_utils::PyValue<Anything>>,
        maximum_value: Option<serde_utils::PyValue<Anything>>,
        pattern: Option<String>,
        structured_pattern: Option<serde_utils::PyValue<PatternExpression>>,
        unit: Option<serde_utils::PyValue<UnitOfMeasure>>,
        implicit_prefix: Option<String>,
        value_presence: Option<PresenceEnum>,
        equals_string: Option<String>,
        equals_string_in: Option<Vec<String>>,
        equals_number: Option<isize>,
        equals_expression: Option<String>,
        exact_cardinality: Option<isize>,
        minimum_cardinality: Option<isize>,
        maximum_cardinality: Option<isize>,
        has_member: Option<serde_utils::PyValue<AnonymousSlotExpression>>,
        all_members: Option<serde_utils::PyValue<AnonymousSlotExpression>>,
        none_of: Option<serde_utils::PyValue<Vec<AnonymousSlotExpression>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<AnonymousSlotExpression>>>,
        any_of: Option<serde_utils::PyValue<Vec<AnonymousSlotExpression>>>,
        all_of: Option<serde_utils::PyValue<Vec<AnonymousSlotExpression>>>,
    ) -> Self {
        let range_expression = range_expression.map(|v| v.into_inner());
        let enum_range = enum_range.map(|v| v.into_inner());
        let bindings = bindings.map(|v| v.into_inner());
        let minimum_value = minimum_value.map(|v| v.into_inner());
        let maximum_value = maximum_value.map(|v| v.into_inner());
        let structured_pattern = structured_pattern.map(|v| v.into_inner());
        let unit = unit.map(|v| v.into_inner());
        let has_member = has_member.map(|v| v.into_inner());
        let all_members = all_members.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        SlotExpression {
            range,
            range_expression,
            enum_range,
            bindings,
            required,
            recommended,
            multivalued,
            inlined,
            inlined_as_list,
            minimum_value,
            maximum_value,
            pattern,
            structured_pattern,
            unit,
            implicit_prefix,
            value_presence,
            equals_string,
            equals_string_in,
            equals_number,
            equals_expression,
            exact_cardinality,
            minimum_cardinality,
            maximum_cardinality,
            has_member,
            all_members,
            none_of,
            exactly_one_of,
            any_of,
            all_of,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<SlotExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<SlotExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<SlotExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid SlotExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum SlotExpressionOrSubtype {
    AnonymousSlotExpression(AnonymousSlotExpression),
    SlotDefinition(SlotDefinition),
}

impl From<AnonymousSlotExpression> for SlotExpressionOrSubtype {
    fn from(x: AnonymousSlotExpression) -> Self {
        Self::AnonymousSlotExpression(x)
    }
}
impl From<SlotDefinition> for SlotExpressionOrSubtype {
    fn from(x: SlotDefinition) -> Self {
        Self::SlotDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for SlotExpressionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousSlotExpression>() {
            return Ok(SlotExpressionOrSubtype::AnonymousSlotExpression(val));
        }
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(SlotExpressionOrSubtype::SlotDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid SlotExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for SlotExpressionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            SlotExpressionOrSubtype::SlotDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<SlotExpressionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<SlotExpressionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<SlotExpressionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid SlotExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    SlotExpressionOrSubtype = AnonymousSlotExpression | SlotDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct AnonymousSlotExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub range: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub range_expression: Option<Box<AnonymousClassExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub enum_range: Option<EnumExpressionOrSubtype>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub bindings: Option<Vec<EnumBinding>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub required: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub recommended: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub multivalued: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub inlined: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub inlined_as_list: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_value: Option<Anything>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub pattern: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_pattern: Option<PatternExpression>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub unit: Option<UnitOfMeasure>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub implicit_prefix: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub value_presence: Option<PresenceEnum>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string_in: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_number: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_expression: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub has_member: Option<Box<AnonymousSlotExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_members: Option<Box<AnonymousSlotExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl AnonymousSlotExpression {
    #[new]
    #[pyo3(signature = (range=None, range_expression=None, enum_range=None, bindings=None, required=None, recommended=None, multivalued=None, inlined=None, inlined_as_list=None, minimum_value=None, maximum_value=None, pattern=None, structured_pattern=None, unit=None, implicit_prefix=None, value_presence=None, equals_string=None, equals_string_in=None, equals_number=None, equals_expression=None, exact_cardinality=None, minimum_cardinality=None, maximum_cardinality=None, has_member=None, all_members=None, none_of=None, exactly_one_of=None, any_of=None, all_of=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        range: Option<String>,
        range_expression: Option<serde_utils::PyValue<Box<AnonymousClassExpression>>>,
        enum_range: Option<serde_utils::PyValue<EnumExpressionOrSubtype>>,
        bindings: Option<serde_utils::PyValue<Vec<EnumBinding>>>,
        required: Option<bool>,
        recommended: Option<bool>,
        multivalued: Option<bool>,
        inlined: Option<bool>,
        inlined_as_list: Option<bool>,
        minimum_value: Option<serde_utils::PyValue<Anything>>,
        maximum_value: Option<serde_utils::PyValue<Anything>>,
        pattern: Option<String>,
        structured_pattern: Option<serde_utils::PyValue<PatternExpression>>,
        unit: Option<serde_utils::PyValue<UnitOfMeasure>>,
        implicit_prefix: Option<String>,
        value_presence: Option<PresenceEnum>,
        equals_string: Option<String>,
        equals_string_in: Option<Vec<String>>,
        equals_number: Option<isize>,
        equals_expression: Option<String>,
        exact_cardinality: Option<isize>,
        minimum_cardinality: Option<isize>,
        maximum_cardinality: Option<isize>,
        has_member: Option<serde_utils::PyValue<Box<AnonymousSlotExpression>>>,
        all_members: Option<serde_utils::PyValue<Box<AnonymousSlotExpression>>>,
        none_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        any_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        all_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let range_expression = range_expression.map(|v| v.into_inner());
        let enum_range = enum_range.map(|v| v.into_inner());
        let bindings = bindings.map(|v| v.into_inner());
        let minimum_value = minimum_value.map(|v| v.into_inner());
        let maximum_value = maximum_value.map(|v| v.into_inner());
        let structured_pattern = structured_pattern.map(|v| v.into_inner());
        let unit = unit.map(|v| v.into_inner());
        let has_member = has_member.map(|v| v.into_inner());
        let all_members = all_members.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        AnonymousSlotExpression {
            range,
            range_expression,
            enum_range,
            bindings,
            required,
            recommended,
            multivalued,
            inlined,
            inlined_as_list,
            minimum_value,
            maximum_value,
            pattern,
            structured_pattern,
            unit,
            implicit_prefix,
            value_presence,
            equals_string,
            equals_string_in,
            equals_number,
            equals_expression,
            exact_cardinality,
            minimum_cardinality,
            maximum_cardinality,
            has_member,
            all_members,
            none_of,
            exactly_one_of,
            any_of,
            all_of,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AnonymousSlotExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AnonymousSlotExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousSlotExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnonymousSlotExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
#[derive(Merge)]
pub struct SlotDefinition {
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub singular_name: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub domain: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_uri: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub array: Option<ArrayExpression>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub inherited: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub readonly: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub ifabsent: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub list_elements_unique: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub list_elements_ordered: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub shared: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub key: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub identifier: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub designates_type: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alias: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub owner: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub domain_of: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub subproperty_of: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub symmetric: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub reflexive: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub locally_reflexive: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub irreflexive: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub asymmetric: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub transitive: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub inverse: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_class_field: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub transitive_form_of: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub reflexive_transitive_form_of: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub role: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_usage_slot: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub usage_slot_name: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub relational_role: Option<RelationalRoleEnum>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_group: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_grouping_slot: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub path_rule: Option<Box<PathExpression>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub disjoint_with: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub children_are_mutually_disjoint: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub union_of: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub type_mappings: Option<HashMap<String, TypeMapping>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub range: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub range_expression: Option<Box<AnonymousClassExpression>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub enum_range: Option<EnumExpressionOrSubtype>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub bindings: Option<Vec<EnumBinding>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub required: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub recommended: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub multivalued: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub inlined: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub inlined_as_list: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_value: Option<Anything>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_value: Option<Anything>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub pattern: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_pattern: Option<PatternExpression>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub unit: Option<UnitOfMeasure>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implicit_prefix: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub value_presence: Option<PresenceEnum>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_string_in: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_number: Option<isize>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub equals_expression: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_cardinality: Option<isize>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_cardinality: Option<isize>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_cardinality: Option<isize>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub has_member: Option<Box<AnonymousSlotExpression>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_members: Option<Box<AnonymousSlotExpression>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<Box<AnonymousSlotExpression>>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_a: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "abstract"))]
    pub abstract_: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixin: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixins: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub apply_to: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub values_from: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub string_serialization: Option<String>,
    #[merge(skip)]
    pub name: String,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[merge(strategy = overwrite_except_none)]
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl SlotDefinition {
    #[new]
    #[pyo3(signature = (name, singular_name=None, domain=None, slot_uri=None, array=None, inherited=None, readonly=None, ifabsent=None, list_elements_unique=None, list_elements_ordered=None, shared=None, key=None, identifier=None, designates_type=None, alias=None, owner=None, domain_of=None, subproperty_of=None, symmetric=None, reflexive=None, locally_reflexive=None, irreflexive=None, asymmetric=None, transitive=None, inverse=None, is_class_field=None, transitive_form_of=None, reflexive_transitive_form_of=None, role=None, is_usage_slot=None, usage_slot_name=None, relational_role=None, slot_group=None, is_grouping_slot=None, path_rule=None, disjoint_with=None, children_are_mutually_disjoint=None, union_of=None, type_mappings=None, range=None, range_expression=None, enum_range=None, bindings=None, required=None, recommended=None, multivalued=None, inlined=None, inlined_as_list=None, minimum_value=None, maximum_value=None, pattern=None, structured_pattern=None, unit=None, implicit_prefix=None, value_presence=None, equals_string=None, equals_string_in=None, equals_number=None, equals_expression=None, exact_cardinality=None, minimum_cardinality=None, maximum_cardinality=None, has_member=None, all_members=None, none_of=None, exactly_one_of=None, any_of=None, all_of=None, is_a=None, abstract_=None, mixin=None, mixins=None, apply_to=None, values_from=None, string_serialization=None, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        name: String,
        singular_name: Option<String>,
        domain: Option<String>,
        slot_uri: Option<uriorcurie>,
        array: Option<serde_utils::PyValue<ArrayExpression>>,
        inherited: Option<bool>,
        readonly: Option<String>,
        ifabsent: Option<String>,
        list_elements_unique: Option<bool>,
        list_elements_ordered: Option<bool>,
        shared: Option<bool>,
        key: Option<bool>,
        identifier: Option<bool>,
        designates_type: Option<bool>,
        alias: Option<String>,
        owner: Option<String>,
        domain_of: Option<Vec<String>>,
        subproperty_of: Option<String>,
        symmetric: Option<bool>,
        reflexive: Option<bool>,
        locally_reflexive: Option<bool>,
        irreflexive: Option<bool>,
        asymmetric: Option<bool>,
        transitive: Option<bool>,
        inverse: Option<String>,
        is_class_field: Option<bool>,
        transitive_form_of: Option<String>,
        reflexive_transitive_form_of: Option<String>,
        role: Option<String>,
        is_usage_slot: Option<bool>,
        usage_slot_name: Option<String>,
        relational_role: Option<RelationalRoleEnum>,
        slot_group: Option<String>,
        is_grouping_slot: Option<bool>,
        path_rule: Option<serde_utils::PyValue<Box<PathExpression>>>,
        disjoint_with: Option<Vec<String>>,
        children_are_mutually_disjoint: Option<bool>,
        union_of: Option<Vec<String>>,
        type_mappings: Option<serde_utils::PyValue<HashMap<String, TypeMapping>>>,
        range: Option<String>,
        range_expression: Option<serde_utils::PyValue<Box<AnonymousClassExpression>>>,
        enum_range: Option<serde_utils::PyValue<EnumExpressionOrSubtype>>,
        bindings: Option<serde_utils::PyValue<Vec<EnumBinding>>>,
        required: Option<bool>,
        recommended: Option<bool>,
        multivalued: Option<bool>,
        inlined: Option<bool>,
        inlined_as_list: Option<bool>,
        minimum_value: Option<serde_utils::PyValue<Anything>>,
        maximum_value: Option<serde_utils::PyValue<Anything>>,
        pattern: Option<String>,
        structured_pattern: Option<serde_utils::PyValue<PatternExpression>>,
        unit: Option<serde_utils::PyValue<UnitOfMeasure>>,
        implicit_prefix: Option<String>,
        value_presence: Option<PresenceEnum>,
        equals_string: Option<String>,
        equals_string_in: Option<Vec<String>>,
        equals_number: Option<isize>,
        equals_expression: Option<String>,
        exact_cardinality: Option<isize>,
        minimum_cardinality: Option<isize>,
        maximum_cardinality: Option<isize>,
        has_member: Option<serde_utils::PyValue<Box<AnonymousSlotExpression>>>,
        all_members: Option<serde_utils::PyValue<Box<AnonymousSlotExpression>>>,
        none_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        any_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        all_of: Option<serde_utils::PyValue<Vec<Box<AnonymousSlotExpression>>>>,
        is_a: Option<String>,
        abstract_: Option<bool>,
        mixin: Option<bool>,
        mixins: Option<Vec<String>>,
        apply_to: Option<Vec<String>>,
        values_from: Option<Vec<uriorcurie>>,
        string_serialization: Option<String>,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let array = array.map(|v| v.into_inner());
        let path_rule = path_rule.map(|v| v.into_inner());
        let type_mappings = type_mappings.map(|v| v.into_inner());
        let range_expression = range_expression.map(|v| v.into_inner());
        let enum_range = enum_range.map(|v| v.into_inner());
        let bindings = bindings.map(|v| v.into_inner());
        let minimum_value = minimum_value.map(|v| v.into_inner());
        let maximum_value = maximum_value.map(|v| v.into_inner());
        let structured_pattern = structured_pattern.map(|v| v.into_inner());
        let unit = unit.map(|v| v.into_inner());
        let has_member = has_member.map(|v| v.into_inner());
        let all_members = all_members.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        SlotDefinition {
            name,
            singular_name,
            domain,
            slot_uri,
            array,
            inherited,
            readonly,
            ifabsent,
            list_elements_unique,
            list_elements_ordered,
            shared,
            key,
            identifier,
            designates_type,
            alias,
            owner,
            domain_of,
            subproperty_of,
            symmetric,
            reflexive,
            locally_reflexive,
            irreflexive,
            asymmetric,
            transitive,
            inverse,
            is_class_field,
            transitive_form_of,
            reflexive_transitive_form_of,
            role,
            is_usage_slot,
            usage_slot_name,
            relational_role,
            slot_group,
            is_grouping_slot,
            path_rule,
            disjoint_with,
            children_are_mutually_disjoint,
            union_of,
            type_mappings,
            range,
            range_expression,
            enum_range,
            bindings,
            required,
            recommended,
            multivalued,
            inlined,
            inlined_as_list,
            minimum_value,
            maximum_value,
            pattern,
            structured_pattern,
            unit,
            implicit_prefix,
            value_presence,
            equals_string,
            equals_string_in,
            equals_number,
            equals_expression,
            exact_cardinality,
            minimum_cardinality,
            maximum_cardinality,
            has_member,
            all_members,
            none_of,
            exactly_one_of,
            any_of,
            all_of,
            is_a,
            abstract_,
            mixin,
            mixins,
            apply_to,
            values_from,
            string_serialization,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<SlotDefinition> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<SlotDefinition> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<SlotDefinition>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid SlotDefinition",
        ))
    }
}

impl SlotDefinition {
    pub fn merge_with(&mut self, other: &SlotDefinition) {
        self.merge(other.clone())
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for SlotDefinition {
    type Key = String;
    type Value = String;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("singular_name".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct ClassExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<AnonymousClassExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<AnonymousClassExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<AnonymousClassExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<AnonymousClassExpression>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_conditions: Option<HashMap<String, SlotDefinition>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl ClassExpression {
    #[new]
    #[pyo3(signature = (any_of=None, exactly_one_of=None, none_of=None, all_of=None, slot_conditions=None))]
    pub fn new(
        any_of: Option<serde_utils::PyValue<Vec<AnonymousClassExpression>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<AnonymousClassExpression>>>,
        none_of: Option<serde_utils::PyValue<Vec<AnonymousClassExpression>>>,
        all_of: Option<serde_utils::PyValue<Vec<AnonymousClassExpression>>>,
        slot_conditions: Option<serde_utils::PyValue<HashMap<String, SlotDefinition>>>,
    ) -> Self {
        let any_of = any_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        let slot_conditions = slot_conditions.map(|v| v.into_inner());
        ClassExpression {
            any_of,
            exactly_one_of,
            none_of,
            all_of,
            slot_conditions,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ClassExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ClassExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ClassExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ClassExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum ClassExpressionOrSubtype {
    AnonymousClassExpression(AnonymousClassExpression),
    ClassDefinition(ClassDefinition),
}

impl From<AnonymousClassExpression> for ClassExpressionOrSubtype {
    fn from(x: AnonymousClassExpression) -> Self {
        Self::AnonymousClassExpression(x)
    }
}
impl From<ClassDefinition> for ClassExpressionOrSubtype {
    fn from(x: ClassDefinition) -> Self {
        Self::ClassDefinition(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for ClassExpressionOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousClassExpression>() {
            return Ok(ClassExpressionOrSubtype::AnonymousClassExpression(val));
        }
        if let Ok(val) = ob.extract::<ClassDefinition>() {
            return Ok(ClassExpressionOrSubtype::ClassDefinition(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ClassExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for ClassExpressionOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ClassExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
            ClassExpressionOrSubtype::ClassDefinition(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ClassExpressionOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ClassExpressionOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ClassExpressionOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ClassExpressionOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(
    ClassExpressionOrSubtype = AnonymousClassExpression | ClassDefinition
);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct AnonymousClassExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_a: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_conditions: Option<HashMap<String, Box<SlotDefinition>>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl AnonymousClassExpression {
    #[new]
    #[pyo3(signature = (is_a=None, any_of=None, exactly_one_of=None, none_of=None, all_of=None, slot_conditions=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        is_a: Option<String>,
        any_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        none_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        all_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        slot_conditions: Option<serde_utils::PyValue<HashMap<String, Box<SlotDefinition>>>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let any_of = any_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        let slot_conditions = slot_conditions.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        AnonymousClassExpression {
            is_a,
            any_of,
            exactly_one_of,
            none_of,
            all_of,
            slot_conditions,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AnonymousClassExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AnonymousClassExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AnonymousClassExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AnonymousClassExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct ClassDefinition {
    #[cfg_attr(feature = "serde", serde(default))]
    pub slots: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_usage: Option<HashMap<String, Box<SlotDefinition>>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub attributes: Option<HashMap<String, Box<SlotDefinition>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub class_uri: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub subclass_of: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub union_of: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub defining_slots: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub tree_root: Option<bool>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub unique_keys: Option<HashMap<String, Box<UniqueKey>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rules: Option<Vec<Box<ClassRule>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub classification_rules: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_names_unique: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub represents_relationship: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub disjoint_with: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub children_are_mutually_disjoint: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub any_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exactly_one_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub none_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub all_of: Option<Vec<Box<AnonymousClassExpression>>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub slot_conditions: Option<HashMap<String, Box<SlotDefinition>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_a: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "abstract"))]
    pub abstract_: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixin: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixins: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub apply_to: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub values_from: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub string_serialization: Option<String>,
    pub name: String,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes: Option<Vec<ncname>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub id_prefixes_are_closed: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition_uri: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub local_names: Option<HashMap<String, LocalName>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conforms_to: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl ClassDefinition {
    #[new]
    #[pyo3(signature = (name, slots=None, slot_usage=None, attributes=None, class_uri=None, subclass_of=None, union_of=None, defining_slots=None, tree_root=None, unique_keys=None, rules=None, classification_rules=None, slot_names_unique=None, represents_relationship=None, disjoint_with=None, children_are_mutually_disjoint=None, any_of=None, exactly_one_of=None, none_of=None, all_of=None, slot_conditions=None, is_a=None, abstract_=None, mixin=None, mixins=None, apply_to=None, values_from=None, string_serialization=None, id_prefixes=None, id_prefixes_are_closed=None, definition_uri=None, local_names=None, conforms_to=None, implements=None, instantiates=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        name: String,
        slots: Option<Vec<String>>,
        slot_usage: Option<serde_utils::PyValue<HashMap<String, Box<SlotDefinition>>>>,
        attributes: Option<serde_utils::PyValue<HashMap<String, Box<SlotDefinition>>>>,
        class_uri: Option<uriorcurie>,
        subclass_of: Option<uriorcurie>,
        union_of: Option<Vec<String>>,
        defining_slots: Option<Vec<String>>,
        tree_root: Option<bool>,
        unique_keys: Option<serde_utils::PyValue<HashMap<String, Box<UniqueKey>>>>,
        rules: Option<serde_utils::PyValue<Vec<Box<ClassRule>>>>,
        classification_rules: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        slot_names_unique: Option<bool>,
        represents_relationship: Option<bool>,
        disjoint_with: Option<Vec<String>>,
        children_are_mutually_disjoint: Option<bool>,
        any_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        exactly_one_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        none_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        all_of: Option<serde_utils::PyValue<Vec<Box<AnonymousClassExpression>>>>,
        slot_conditions: Option<serde_utils::PyValue<HashMap<String, Box<SlotDefinition>>>>,
        is_a: Option<String>,
        abstract_: Option<bool>,
        mixin: Option<bool>,
        mixins: Option<Vec<String>>,
        apply_to: Option<Vec<String>>,
        values_from: Option<Vec<uriorcurie>>,
        string_serialization: Option<String>,
        id_prefixes: Option<Vec<ncname>>,
        id_prefixes_are_closed: Option<bool>,
        definition_uri: Option<uriorcurie>,
        local_names: Option<serde_utils::PyValue<HashMap<String, LocalName>>>,
        conforms_to: Option<String>,
        implements: Option<Vec<uriorcurie>>,
        instantiates: Option<Vec<uriorcurie>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let slot_usage = slot_usage.map(|v| v.into_inner());
        let attributes = attributes.map(|v| v.into_inner());
        let unique_keys = unique_keys.map(|v| v.into_inner());
        let rules = rules.map(|v| v.into_inner());
        let classification_rules = classification_rules.map(|v| v.into_inner());
        let any_of = any_of.map(|v| v.into_inner());
        let exactly_one_of = exactly_one_of.map(|v| v.into_inner());
        let none_of = none_of.map(|v| v.into_inner());
        let all_of = all_of.map(|v| v.into_inner());
        let slot_conditions = slot_conditions.map(|v| v.into_inner());
        let local_names = local_names.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        ClassDefinition {
            name,
            slots,
            slot_usage,
            attributes,
            class_uri,
            subclass_of,
            union_of,
            defining_slots,
            tree_root,
            unique_keys,
            rules,
            classification_rules,
            slot_names_unique,
            represents_relationship,
            disjoint_with,
            children_are_mutually_disjoint,
            any_of,
            exactly_one_of,
            none_of,
            all_of,
            slot_conditions,
            is_a,
            abstract_,
            mixin,
            mixins,
            apply_to,
            values_from,
            string_serialization,
            id_prefixes,
            id_prefixes_are_closed,
            definition_uri,
            local_names,
            conforms_to,
            implements,
            instantiates,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ClassDefinition> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ClassDefinition> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ClassDefinition>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ClassDefinition",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for ClassDefinition {
    type Key = String;
    type Value = uriorcurie;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("name".into()), Value::String(k));
        map.insert(Value::String("class_uri".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct ClassLevelRule {}
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum ClassLevelRuleOrSubtype {
    ClassRule(ClassRule),
}

impl From<ClassRule> for ClassLevelRuleOrSubtype {
    fn from(x: ClassRule) -> Self {
        Self::ClassRule(x)
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for ClassLevelRuleOrSubtype {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ClassRule>() {
            return Ok(ClassLevelRuleOrSubtype::ClassRule(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ClassLevelRuleOrSubtype",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for ClassLevelRuleOrSubtype {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ClassLevelRuleOrSubtype::ClassRule(val) => {
                val.into_pyobject(py).map(move |b| b.into_any())
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ClassLevelRuleOrSubtype> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ClassLevelRuleOrSubtype> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ClassLevelRuleOrSubtype>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ClassLevelRuleOrSubtype",
        ))
    }
}

#[cfg(feature = "stubgen")]
::pyo3_stub_gen::impl_stub_type!(ClassLevelRuleOrSubtype = ClassRule);

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct ClassRule {
    #[cfg_attr(feature = "serde", serde(default))]
    pub preconditions: Option<Box<AnonymousClassExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub postconditions: Option<Box<AnonymousClassExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub elseconditions: Option<Box<AnonymousClassExpression>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub bidirectional: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub open_world: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deactivated: Option<bool>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl ClassRule {
    #[new]
    #[pyo3(signature = (preconditions=None, postconditions=None, elseconditions=None, bidirectional=None, open_world=None, rank=None, deactivated=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, categories=None, keywords=None))]
    pub fn new(
        preconditions: Option<serde_utils::PyValue<Box<AnonymousClassExpression>>>,
        postconditions: Option<serde_utils::PyValue<Box<AnonymousClassExpression>>>,
        elseconditions: Option<serde_utils::PyValue<Box<AnonymousClassExpression>>>,
        bidirectional: Option<bool>,
        open_world: Option<bool>,
        rank: Option<isize>,
        deactivated: Option<bool>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let preconditions = preconditions.map(|v| v.into_inner());
        let postconditions = postconditions.map(|v| v.into_inner());
        let elseconditions = elseconditions.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        ClassRule {
            preconditions,
            postconditions,
            elseconditions,
            bidirectional,
            open_world,
            rank,
            deactivated,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ClassRule> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ClassRule> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ClassRule>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ClassRule",
        ))
    }
}

pub mod array_expression_utl {
    use super::*;
    #[derive(Debug, Clone, PartialEq)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum maximum_number_dimensions_range {
        Anything(Anything),
        isize(isize),
        bool(bool),
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for maximum_number_dimensions_range {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<Anything>() {
                return Ok(maximum_number_dimensions_range::Anything(val));
            }
            if let Ok(val) = ob.extract::<isize>() {
                return Ok(maximum_number_dimensions_range::isize(val));
            }
            if let Ok(val) = ob.extract::<bool>() {
                return Ok(maximum_number_dimensions_range::bool(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid maximum_number_dimensions",
            ))
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for maximum_number_dimensions_range {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                maximum_number_dimensions_range::Anything(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                maximum_number_dimensions_range::isize(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
                maximum_number_dimensions_range::bool(val) => Ok(val
                    .into_pyobject(py)
                    .map(move |b| <pyo3::Bound<'_, _> as Clone>::clone(&b).into_any())?),
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> IntoPyObject<'py> for Box<maximum_number_dimensions_range> {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;
        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (*self).into_pyobject(py).map(move |x| x.into_any())
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> FromPyObject<'py> for Box<maximum_number_dimensions_range> {
        fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
            if let Ok(val) = ob.extract::<maximum_number_dimensions_range>() {
                return Ok(Box::new(val));
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "invalid maximum_number_dimensions",
            ))
        }
    }

    #[cfg(feature = "stubgen")]
    ::pyo3_stub_gen::impl_stub_type!(maximum_number_dimensions_range = Anything | isize | bool);
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct ArrayExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_number_dimensions: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_number_dimensions: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_number_dimensions: Option<array_expression_utl::maximum_number_dimensions_range>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub dimensions: Option<Vec<DimensionExpression>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl ArrayExpression {
    #[new]
    #[pyo3(signature = (exact_number_dimensions=None, minimum_number_dimensions=None, maximum_number_dimensions=None, dimensions=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        exact_number_dimensions: Option<isize>,
        minimum_number_dimensions: Option<isize>,
        maximum_number_dimensions: Option<array_expression_utl::maximum_number_dimensions_range>,
        dimensions: Option<serde_utils::PyValue<Vec<DimensionExpression>>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let dimensions = dimensions.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        ArrayExpression {
            exact_number_dimensions,
            minimum_number_dimensions,
            maximum_number_dimensions,
            dimensions,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ArrayExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ArrayExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ArrayExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ArrayExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct DimensionExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub alias: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub maximum_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub minimum_cardinality: Option<isize>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_cardinality: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl DimensionExpression {
    #[new]
    #[pyo3(signature = (alias=None, maximum_cardinality=None, minimum_cardinality=None, exact_cardinality=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        alias: Option<String>,
        maximum_cardinality: Option<isize>,
        minimum_cardinality: Option<isize>,
        exact_cardinality: Option<isize>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        DimensionExpression {
            alias,
            maximum_cardinality,
            minimum_cardinality,
            exact_cardinality,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<DimensionExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<DimensionExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<DimensionExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid DimensionExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct PatternExpression {
    #[cfg_attr(feature = "serde", serde(default))]
    pub syntax: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub interpolated: Option<bool>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub partial_match: Option<bool>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PatternExpression {
    #[new]
    #[pyo3(signature = (syntax=None, interpolated=None, partial_match=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        syntax: Option<String>,
        interpolated: Option<bool>,
        partial_match: Option<bool>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        PatternExpression {
            syntax,
            interpolated,
            partial_match,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<PatternExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<PatternExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<PatternExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid PatternExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct ImportExpression {
    pub import_from: uriorcurie,
    #[cfg_attr(feature = "serde", serde(default))]
    pub import_as: Option<ncname>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub import_map: Option<HashMap<String, Setting>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl ImportExpression {
    #[new]
    #[pyo3(signature = (import_from, import_as=None, import_map=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        import_from: uriorcurie,
        import_as: Option<ncname>,
        import_map: Option<serde_utils::PyValue<HashMap<String, Setting>>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let import_map = import_map.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        ImportExpression {
            import_from,
            import_as,
            import_map,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<ImportExpression> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<ImportExpression> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<ImportExpression>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid ImportExpression",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Setting {
    pub setting_key: ncname,
    pub setting_value: String,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Setting {
    #[new]
    #[pyo3(signature = (setting_key, setting_value))]
    pub fn new(setting_key: ncname, setting_value: String) -> Self {
        Setting {
            setting_key,
            setting_value,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Setting> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Setting> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Setting>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Setting",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for Setting {
    type Key = ncname;
    type Value = String;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.setting_key;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("setting_key".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("setting_key".into()), Value::String(k));
        map.insert(Value::String("setting_value".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Prefix {
    pub prefix_prefix: ncname,
    pub prefix_reference: uri,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Prefix {
    #[new]
    #[pyo3(signature = (prefix_prefix, prefix_reference))]
    pub fn new(prefix_prefix: ncname, prefix_reference: uri) -> Self {
        Prefix {
            prefix_prefix,
            prefix_reference,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Prefix> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Prefix> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Prefix>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Prefix",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for Prefix {
    type Key = ncname;
    type Value = uri;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.prefix_prefix;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("prefix_prefix".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("prefix_prefix".into()), Value::String(k));
        map.insert(Value::String("prefix_reference".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct LocalName {
    pub local_name_source: ncname,
    pub local_name_value: String,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl LocalName {
    #[new]
    #[pyo3(signature = (local_name_source, local_name_value))]
    pub fn new(local_name_source: ncname, local_name_value: String) -> Self {
        LocalName {
            local_name_source,
            local_name_value,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<LocalName> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<LocalName> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<LocalName>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid LocalName",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for LocalName {
    type Key = ncname;
    type Value = String;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.local_name_source;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("local_name_source".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("local_name_source".into()), Value::String(k));
        map.insert(Value::String("local_name_value".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct Example {
    #[cfg_attr(feature = "serde", serde(default))]
    pub value: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "description"))]
    pub value_description: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "object"))]
    pub value_object: Option<Anything>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Example {
    #[new]
    #[pyo3(signature = (value=None, value_description=None, value_object=None))]
    pub fn new(
        value: Option<String>,
        value_description: Option<String>,
        value_object: Option<serde_utils::PyValue<Anything>>,
    ) -> Self {
        let value_object = value_object.map(|v| v.into_inner());
        Example {
            value,
            value_description,
            value_object,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<Example> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<Example> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<Example>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid Example",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct AltDescription {
    #[cfg_attr(feature = "serde", serde(alias = "source"))]
    pub alt_description_source: String,
    #[cfg_attr(feature = "serde", serde(alias = "description"))]
    pub alt_description_text: String,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl AltDescription {
    #[new]
    #[pyo3(signature = (alt_description_source, alt_description_text))]
    pub fn new(alt_description_source: String, alt_description_text: String) -> Self {
        AltDescription {
            alt_description_source,
            alt_description_text,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<AltDescription> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<AltDescription> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<AltDescription>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid AltDescription",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for AltDescription {
    type Key = String;
    type Value = String;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.alt_description_source;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(
            Value::String("alt_description_source".into()),
            Value::String(k),
        );
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(
            Value::String("alt_description_source".into()),
            Value::String(k),
        );
        map.insert(Value::String("alt_description_text".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct PermissibleValue {
    pub text: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub meaning: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub unit: Option<UnitOfMeasure>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub instantiates: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub implements: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_a: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mixins: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PermissibleValue {
    #[new]
    #[pyo3(signature = (text, description=None, meaning=None, unit=None, instantiates=None, implements=None, is_a=None, mixins=None, extensions=None, annotations=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        text: String,
        description: Option<String>,
        meaning: Option<uriorcurie>,
        unit: Option<serde_utils::PyValue<UnitOfMeasure>>,
        instantiates: Option<Vec<uriorcurie>>,
        implements: Option<Vec<uriorcurie>>,
        is_a: Option<String>,
        mixins: Option<Vec<String>>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let unit = unit.map(|v| v.into_inner());
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        PermissibleValue {
            text,
            description,
            meaning,
            unit,
            instantiates,
            implements,
            is_a,
            mixins,
            extensions,
            annotations,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<PermissibleValue> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<PermissibleValue> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<PermissibleValue>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid PermissibleValue",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for PermissibleValue {
    type Key = String;
    type Value = String;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.text;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("text".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("text".into()), Value::String(k));
        map.insert(Value::String("description".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct UniqueKey {
    pub unique_key_name: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub unique_key_slots: Vec<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub consider_nulls_inequal: Option<bool>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl UniqueKey {
    #[new]
    #[pyo3(signature = (unique_key_name, unique_key_slots, consider_nulls_inequal=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        unique_key_name: String,
        unique_key_slots: Vec<String>,
        consider_nulls_inequal: Option<bool>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        UniqueKey {
            unique_key_name,
            unique_key_slots,
            consider_nulls_inequal,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<UniqueKey> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<UniqueKey> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<UniqueKey>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid UniqueKey",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for UniqueKey {
    type Key = String;
    type Value = bool;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.unique_key_name;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("unique_key_name".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("unique_key_name".into()), Value::String(k));
        map.insert(Value::String("consider_nulls_inequal".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
pub struct TypeMapping {
    #[cfg_attr(feature = "serde", serde(alias = "framework"))]
    pub framework_key: String,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "serde", serde(alias = "type"))]
    pub mapped_type: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub string_serialization: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub extensions: Option<HashMap<String, ExtensionOrSubtype>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub annotations: Option<HashMap<String, Annotation>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub alt_descriptions: Option<HashMap<String, AltDescription>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub todos: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub notes: Option<Vec<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub comments: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub examples: Option<Vec<Example>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_subset: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub from_schema: Option<uri>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub imported_from: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub source: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub in_language: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub see_also: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_exact_replacement: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deprecated_element_has_possible_replacement: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub aliases: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub structured_aliases: Option<Vec<StructuredAlias>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub exact_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub related_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub narrow_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub broad_mappings: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by: Option<uriorcurie>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub contributors: Option<Vec<uriorcurie>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_on: Option<NaiveDateTime>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub modified_by: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rank: Option<isize>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub categories: Option<Vec<uriorcurie>>,
    #[cfg_attr(
        feature = "serde",
        serde(
            deserialize_with = "serde_utils::deserialize_primitive_list_or_single_value_optional"
        )
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Option<Vec<String>>,
}
#[cfg(feature = "pyo3")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl TypeMapping {
    #[new]
    #[pyo3(signature = (framework_key, mapped_type=None, string_serialization=None, extensions=None, annotations=None, description=None, alt_descriptions=None, title=None, deprecated=None, todos=None, notes=None, comments=None, examples=None, in_subset=None, from_schema=None, imported_from=None, source=None, in_language=None, see_also=None, deprecated_element_has_exact_replacement=None, deprecated_element_has_possible_replacement=None, aliases=None, structured_aliases=None, mappings=None, exact_mappings=None, close_mappings=None, related_mappings=None, narrow_mappings=None, broad_mappings=None, created_by=None, contributors=None, created_on=None, last_updated_on=None, modified_by=None, status=None, rank=None, categories=None, keywords=None))]
    pub fn new(
        framework_key: String,
        mapped_type: Option<String>,
        string_serialization: Option<String>,
        extensions: Option<serde_utils::PyValue<HashMap<String, ExtensionOrSubtype>>>,
        annotations: Option<serde_utils::PyValue<HashMap<String, Annotation>>>,
        description: Option<String>,
        alt_descriptions: Option<serde_utils::PyValue<HashMap<String, AltDescription>>>,
        title: Option<String>,
        deprecated: Option<String>,
        todos: Option<Vec<String>>,
        notes: Option<Vec<String>>,
        comments: Option<Vec<String>>,
        examples: Option<serde_utils::PyValue<Vec<Example>>>,
        in_subset: Option<Vec<String>>,
        from_schema: Option<uri>,
        imported_from: Option<String>,
        source: Option<uriorcurie>,
        in_language: Option<String>,
        see_also: Option<Vec<uriorcurie>>,
        deprecated_element_has_exact_replacement: Option<uriorcurie>,
        deprecated_element_has_possible_replacement: Option<uriorcurie>,
        aliases: Option<Vec<String>>,
        structured_aliases: Option<serde_utils::PyValue<Vec<StructuredAlias>>>,
        mappings: Option<Vec<uriorcurie>>,
        exact_mappings: Option<Vec<uriorcurie>>,
        close_mappings: Option<Vec<uriorcurie>>,
        related_mappings: Option<Vec<uriorcurie>>,
        narrow_mappings: Option<Vec<uriorcurie>>,
        broad_mappings: Option<Vec<uriorcurie>>,
        created_by: Option<uriorcurie>,
        contributors: Option<Vec<uriorcurie>>,
        created_on: Option<NaiveDateTime>,
        last_updated_on: Option<NaiveDateTime>,
        modified_by: Option<uriorcurie>,
        status: Option<uriorcurie>,
        rank: Option<isize>,
        categories: Option<Vec<uriorcurie>>,
        keywords: Option<Vec<String>>,
    ) -> Self {
        let extensions = extensions.map(|v| v.into_inner());
        let annotations = annotations.map(|v| v.into_inner());
        let alt_descriptions = alt_descriptions.map(|v| v.into_inner());
        let examples = examples.map(|v| v.into_inner());
        let structured_aliases = structured_aliases.map(|v| v.into_inner());
        TypeMapping {
            framework_key,
            mapped_type,
            string_serialization,
            extensions,
            annotations,
            description,
            alt_descriptions,
            title,
            deprecated,
            todos,
            notes,
            comments,
            examples,
            in_subset,
            from_schema,
            imported_from,
            source,
            in_language,
            see_also,
            deprecated_element_has_exact_replacement,
            deprecated_element_has_possible_replacement,
            aliases,
            structured_aliases,
            mappings,
            exact_mappings,
            close_mappings,
            related_mappings,
            narrow_mappings,
            broad_mappings,
            created_by,
            contributors,
            created_on,
            last_updated_on,
            modified_by,
            status,
            rank,
            categories,
            keywords,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> IntoPyObject<'py> for Box<TypeMapping> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (*self).into_pyobject(py).map(move |x| x.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> FromPyObject<'py> for Box<TypeMapping> {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(val) = ob.extract::<TypeMapping>() {
            return Ok(Box::new(val));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "invalid TypeMapping",
        ))
    }
}

#[cfg(feature = "serde")]
impl serde_utils::InlinedPair for TypeMapping {
    type Key = String;
    type Value = TypeDefinition;
    type Error = String;

    fn extract_key(&self) -> &Self::Key {
        return &self.framework_key;
    }

    fn from_pair_mapping(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map = match v {
            Value::Map(m) => m,
            _ => return Err("ClassDefinition must be a mapping".into()),
        };
        map.insert(Value::String("framework_key".into()), Value::String(k));
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }

    fn from_pair_simple(k: Self::Key, v: Value) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String("framework_key".into()), Value::String(k));
        map.insert(Value::String("mapped_type".into()), v);
        let de = Value::Map(map).into_deserializer();
        match serde_path_to_error::deserialize(de) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(format!("at `{}`: {}", e.path(), e.inner())),
        }
    }
}

/// Overwrite `left` with `right` unless `right` is `None`.
fn overwrite_except_none<T>(left: &mut Option<T>, right: Option<T>) {
    if right.is_some() {
        *left = right;
    }
}

#[cfg(feature = "stubgen")]
define_stub_info_gatherer!(stub_info);
