//! Covers `default_curi_maps:` and builtin `linkml:` import prefix
//! resolution (see `identifier::builtin_prefix_contributions`).
//!
//! Before this fix, a schema that relied on either mechanism to resolve a
//! prefix like `schema:`/`owl:` — instead of declaring it inline under
//! `prefixes:` — failed `SchemaView::add_schema` outright with
//! `CurieError(NotFound(..))` the moment it indexed a class/slot using that
//! prefix, since `converter_from_schemas` only ever read inline `prefixes:`
//! plus a 3-entry hardcoded fallback (`rdfs`/`rdf`/`dcterms`).

use linkml_meta::SchemaDefinition;
use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
use linkml_schemaview::schemaview::SchemaView;

fn schema_from_yaml(yaml: &str) -> SchemaDefinition {
    serde_yml::from_str(yaml).expect("test schema should parse")
}

/// A schema whose `id`/`name` slot uses `slot_uri: schema:identifier`
/// without declaring `schema:` inline, relying entirely on
/// `imports: [linkml:types]` to supply it — the exact shape that used to
/// fail `add_schema` (reproduced from a real-world fixture that hit this).
const IMPORT_ONLY_SCHEMA: &str = r#"
id: https://example.org/import-only
name: import_only
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
default_range: string
classes:
  Entity:
    slots:
      - id
slots:
  id:
    identifier: true
    slot_uri: schema:identifier
"#;

#[test]
fn builtin_linkml_types_import_resolves_schema_prefix() {
    let schema = schema_from_yaml(IMPORT_ONLY_SCHEMA);
    let mut sv = SchemaView::new();
    // This used to fail with CurieError(NotFound("schema")) — `schema:` was
    // unreachable without the (network-only) `resolve` feature having
    // already loaded `linkml:types` as a separate schema.
    sv.add_schema(schema.clone())
        .expect("add_schema should succeed once `schema:` is resolved via the builtin import");

    let conv = converter_from_schemas([&schema]);
    let uri = Identifier::new("schema:identifier")
        .to_uri(&conv)
        .expect("schema: should resolve via the bundled linkml:types prefixes");
    assert_eq!(uri.0, "http://schema.org/identifier");
}

/// A schema relying on `default_curi_maps: [semweb_context]` alone (no
/// import) to resolve `owl:`, which is not in the 3-entry hardcoded
/// fallback (`rdfs`/`rdf`/`dcterms`).
const CURI_MAP_ONLY_SCHEMA: &str = r#"
id: https://example.org/curimap-only
name: curimap_only
prefixes:
  linkml: https://w3id.org/linkml/
default_curi_maps:
  - semweb_context
default_range: string
classes:
  Entity:
    slots:
      - same_as
slots:
  same_as:
    slot_uri: owl:sameAs
"#;

#[test]
fn default_curi_maps_resolves_semweb_context_prefix() {
    let schema = schema_from_yaml(CURI_MAP_ONLY_SCHEMA);
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone())
        .expect("add_schema should succeed once owl: is resolved via default_curi_maps");

    let conv = converter_from_schemas([&schema]);
    let uri = Identifier::new("owl:sameAs")
        .to_uri(&conv)
        .expect("owl: should resolve via the bundled semweb_context map");
    assert_eq!(uri.0, "http://www.w3.org/2002/07/owl#sameAs");
}

/// An explicit `prefixes:` entry must win over a same-keyed prefix that
/// would otherwise be contributed by a builtin import — mirrors
/// `linkml_runtime.Namespaces.add_prefixmap`'s `k not in self` precedence.
const EXPLICIT_OVERRIDE_SCHEMA: &str = r#"
id: https://example.org/explicit-override
name: explicit_override
prefixes:
  linkml: https://w3id.org/linkml/
  schema: https://custom.example.org/schema#
imports:
  - linkml:types
default_range: string
classes:
  Entity:
    slots:
      - id
slots:
  id:
    identifier: true
    slot_uri: schema:identifier
"#;

#[test]
fn explicit_prefix_wins_over_builtin_import_contribution() {
    let schema = schema_from_yaml(EXPLICIT_OVERRIDE_SCHEMA);
    let conv = converter_from_schemas([&schema]);
    let uri = Identifier::new("schema:identifier")
        .to_uri(&conv)
        .expect("schema: is declared explicitly, should always resolve");
    assert_eq!(uri.0, "https://custom.example.org/schema#identifier");
}

/// The fully-expanded URI form of a builtin import (as opposed to the CURIE
/// shorthand) must be recognised too.
const EXPANDED_URI_IMPORT_SCHEMA: &str = r#"
id: https://example.org/expanded-import
name: expanded_import
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - https://w3id.org/linkml/types
default_range: string
classes:
  Entity:
    slots:
      - id
slots:
  id:
    identifier: true
    slot_uri: schema:identifier
"#;

#[test]
fn builtin_import_matches_expanded_uri_form() {
    let schema = schema_from_yaml(EXPANDED_URI_IMPORT_SCHEMA);
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone())
        .expect("add_schema should succeed with the fully-expanded import URI too");

    let conv = converter_from_schemas([&schema]);
    let uri = Identifier::new("schema:identifier").to_uri(&conv).unwrap();
    assert_eq!(uri.0, "http://schema.org/identifier");
}

/// A schema with neither `default_curi_maps` nor a builtin import is
/// unaffected — no behaviour change for the common case.
const PLAIN_SCHEMA: &str = r#"
id: https://example.org/plain
name: plain
prefixes:
  linkml: https://w3id.org/linkml/
  ex: https://example.org/ex#
default_range: string
classes:
  Entity:
    slots:
      - id
slots:
  id:
    identifier: true
    slot_uri: ex:id
"#;

#[test]
fn plain_schema_without_builtins_is_unaffected() {
    let schema = schema_from_yaml(PLAIN_SCHEMA);
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).expect("plain schema loads");

    let conv = converter_from_schemas([&schema]);
    let uri = Identifier::new("ex:id").to_uri(&conv).unwrap();
    assert_eq!(uri.0, "https://example.org/ex#id");

    // An unrelated builtin-only prefix must NOT leak in from nowhere.
    assert!(Identifier::new("schema:identifier").to_uri(&conv).is_err());
}
