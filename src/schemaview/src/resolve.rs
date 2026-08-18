use crate::{
    io::{from_uri, from_yaml},
    schemaview::SchemaView,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn get_uri_for_id(id: &str) -> Option<&'static str> {
    match id {
        "https://w3id.org/linkml/mappings" => Some("https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/mappings.yaml"),
        "https://w3id.org/linkml/types" => Some("https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/types.yaml"),
        "https://w3id.org/linkml/extensions" => Some("https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/extensions.yaml"),
        "https://w3id.org/linkml/annotations" => Some("https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/annotations.yaml"),
        "https://w3id.org/linkml/units" => Some("https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/units.yaml"),
        _ => None,
    }
}

/// Directory a schema's relative imports should be resolved against, given the
/// path the schema itself was loaded from. Accepts either the schema file or a
/// directory, so callers can pass whichever they have.
fn source_dir_of(path: &Path) -> Option<PathBuf> {
    let canonical = std::fs::canonicalize(path).ok()?;
    if canonical.is_dir() {
        Some(canonical)
    } else {
        canonical.parent().map(|p| p.to_path_buf())
    }
}

/// Returns `path` if it names an existing file, retrying with a `.yaml`/`.yml`
/// extension — LinkML imports habitually omit it (`imports: [./types]`).
fn existing_schema_file(path: PathBuf) -> Option<PathBuf> {
    if path.is_file() {
        return Some(path);
    }
    for ext in ["yaml", "yml"] {
        let candidate = path.with_extension(ext);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Locates the file behind a relative import of `schema_id`.
///
/// Bases are tried in order of how much we actually know:
/// 1. the directory of the file `schema_id` was itself loaded from,
/// 2. the directory of the import URI that pulled `schema_id` in,
/// 3. the process working directory — the historical behaviour, kept last so
///    that schemas importing CWD-relative paths keep resolving.
fn locate_import(
    sv: &SchemaView,
    schema_id: &str,
    uri: &str,
    source_dirs: &HashMap<String, PathBuf>,
) -> Option<PathBuf> {
    let raw = Path::new(uri);
    if raw.is_absolute() {
        return existing_schema_file(raw.to_path_buf());
    }
    let mut bases: Vec<PathBuf> = Vec::new();
    if let Some(dir) = source_dirs.get(schema_id) {
        bases.push(dir.clone());
    }
    if let Some(resolution_uri) = sv.get_resolution_uri_of_schema(schema_id) {
        if let Some(parent) = Path::new(&resolution_uri).parent() {
            bases.push(parent.to_path_buf());
        }
    }
    bases.push(PathBuf::new());
    for base in bases {
        if let Some(found) = existing_schema_file(base.join(raw)) {
            return Some(found);
        }
    }
    None
}

/// Loads the schema behind one unresolved import and records where it came
/// from, so that schema's own relative imports can be resolved in a later round.
fn resolve_one(
    sv: &mut SchemaView,
    schema_id: &str,
    uri: &str,
    source_dirs: &mut HashMap<String, PathBuf>,
) -> Result<(), String> {
    let import_ref = Some((schema_id.to_string(), uri.to_string()));

    if let Some(resolved_uri) = get_uri_for_id(uri) {
        let schema = from_uri(resolved_uri)
            .map_err(|e| format!("Failed to load schema from {}: {}", resolved_uri, e))?;
        sv.add_schema_with_import_ref(schema, import_ref)?;
        return Ok(());
    }

    let Some(path) = locate_import(sv, schema_id, uri, source_dirs) else {
        return Err(format!(
            "No resolution found for URI: {} imported from {}",
            uri,
            source_dirs
                .get(schema_id)
                .map(|d| d.display().to_string())
                .or_else(|| sv.get_resolution_uri_of_schema(schema_id))
                .unwrap_or_else(|| schema_id.to_owned())
        ));
    };

    let schema = from_yaml(&path)
        .map_err(|e| format!("Failed to load schema from {}: {}", path.display(), e))?;
    let loaded_id = schema.id.clone();
    sv.add_schema_with_import_ref(schema, import_ref)?;
    if let Some(dir) = source_dir_of(&path) {
        source_dirs.insert(loaded_id, dir);
    }
    Ok(())
}

/// Resolves imports repeatedly until nothing is left or a round achieves
/// nothing, so that imports-of-imports are reached.
///
/// A failure no longer abandons the rest of the pass: every import is attempted
/// each round, and only when a round resolves nothing do the accumulated
/// messages become the error. An import that failed while other work was still
/// progressing gets retried, because that later work may be exactly what makes
/// it resolvable.
fn resolve_to_fixpoint(
    sv: &mut SchemaView,
    mut source_dirs: HashMap<String, PathBuf>,
) -> Result<(), String> {
    loop {
        let unresolved = sv.get_unresolved_schemas();
        if unresolved.is_empty() {
            return Ok(());
        }
        let mut progressed = false;
        let mut failures: Vec<String> = Vec::new();
        for (schema_id, uri) in unresolved {
            match resolve_one(sv, &schema_id, &uri, &mut source_dirs) {
                Ok(()) => progressed = true,
                Err(e) => failures.push(e),
            }
        }
        if !progressed {
            return Err(if failures.is_empty() {
                "import resolution stalled with imports still unresolved".to_string()
            } else {
                failures.join("\n")
            });
        }
    }
}

/// Resolves every import reachable from the schemas already in `sv`.
///
/// Relative imports of those schemas are resolved against the process working
/// directory, because nothing records where they were loaded from. Prefer
/// [`resolve_schemas_from`], which removes that guess.
pub fn resolve_schemas(sv: &mut SchemaView) -> Result<(), String> {
    resolve_to_fixpoint(sv, HashMap::new())
}

/// Resolves every import reachable from the schemas already in `sv`, treating
/// `root_source` — the path they were loaded from — as the base for their
/// relative imports. Schemas pulled in during resolution get the same treatment
/// from their own file, so `a.yaml` importing `./b` works at any depth and from
/// any working directory.
///
/// `root_source` may be the schema file or its directory.
pub fn resolve_schemas_from(sv: &mut SchemaView, root_source: &Path) -> Result<(), String> {
    let mut source_dirs = HashMap::new();
    if let Some(dir) = source_dir_of(root_source) {
        // Everything currently in the view came from `root_source`: the callers
        // load exactly one schema before resolving.
        sv.with_schema_definitions(|schemas| {
            for id in schemas.keys() {
                source_dirs.insert(id.clone(), dir.clone());
            }
        });
    }
    resolve_to_fixpoint(sv, source_dirs)
}
