use linkml_meta::SchemaDefinition;
use serde_yaml_ng;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub fn from_yaml(path: &Path) -> Result<SchemaDefinition, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let schema = serde_yaml_ng::Deserializer::from_reader(reader);
    let result: Result<SchemaDefinition, _> = serde_path_to_error::deserialize(schema);

    Ok(result?)
}

#[cfg(feature = "resolve")]
pub fn from_uri(uri: &str) -> Result<SchemaDefinition, Box<dyn Error>> {
    let r = reqwest::blocking::get(uri)?;
    let reader = r.text()?;
    let schema = serde_yaml_ng::Deserializer::from_str(&reader);
    let result: Result<SchemaDefinition, _> = serde_path_to_error::deserialize(schema);
    Ok(result?)
}

#[cfg(test)]
mod tests {
    use crate::schemaview::SchemaView;
    use linkml_meta::poly::SchemaDefinition as _;
    use linkml_meta::poly_containers::MapRef;
    use std::path::{absolute, PathBuf};

    pub fn meta_path() -> PathBuf {
        let mut this_file = PathBuf::from("src/io.rs");
        this_file = absolute(this_file.as_path()).unwrap();
        this_file.pop();
        this_file.pop();
        this_file.push("tests");
        this_file.push("data");
        this_file.push("meta.yaml");
        this_file
    }

    use super::*;
    #[test]
    fn test_load_schema() {
        let path = &meta_path();

        let schema = match from_yaml(path) {
            Ok(v) => v,
            Err(why) => panic!("{why:?}"),
        };

        if let Some(classes) = schema.classes() {
            for (name, class) in classes.iter() {
                let is_a = &class.is_a;
                match is_a {
                    None => println!("class: {name} has no superclass"),
                    Some(is_a) => {
                        println!("class: {name} is a subclass of {is_a:?}")
                    }
                }
            }
        } else {
            println!("No classes found");
        };
    }

    #[cfg(feature = "resolve")]
    use crate::resolve::resolve_schemas;

    #[cfg(feature = "resolve")]
    #[test]
    fn test_resolve_schemas() {
        let path = &meta_path();

        let schema = match from_yaml(path) {
            Ok(v) => v,
            Err(why) => panic!("{why:?}"),
        };
        let mut schema_view = SchemaView::new();
        schema_view.add_schema(schema).unwrap();
        let unresolved = schema_view.get_unresolved_schemas();
        assert!(unresolved
            .iter()
            .map(|x| x.1.clone())
            .collect::<Vec<_>>()
            .contains(&"https://w3id.org/linkml/mappings".to_string()));
        println!("Unresolved schemas: {:?}", unresolved);
        resolve_schemas(&mut schema_view).unwrap();
        let unresolved = schema_view.get_unresolved_schemas();
        assert!(unresolved.is_empty());
    }
}
