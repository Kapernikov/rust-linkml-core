use std::collections::HashMap;

use crate::identifier::{converter_from_schema, Identifier, IdentifierError};
use curies::Converter;
use linkml_meta::{ClassDefinition, SchemaDefinition, SlotDefinition, TypeDefinition};

use crate::curie::curie2uri;

pub struct SchemaView {
    schema_definitions: HashMap<String, SchemaDefinition>,
    primary_schema: Option<String>,
    class_uri_index: HashMap<String, (String, String)>,
}

pub struct ClassView<'a> {
    pub class: &'a ClassDefinition,
    schema: &'a SchemaDefinition,
}

impl<'a> ClassView<'a> {
    pub fn new(class: &'a ClassDefinition, schema: &'a SchemaDefinition) -> Self {
        Self { class, schema }
    }

    pub fn get_uri(
        &self,
        conv: &Converter,
        native: bool,
        expand: bool,
    ) -> Result<Identifier, IdentifierError> {
        let prefix = self
            .schema
            .default_prefix
            .as_deref()
            .unwrap_or(&self.schema.name);
        let curie = format!("{}:{}", prefix, self.class.name);
        let mut id = Identifier::new(&curie);
        if !native {
            if let Some(curi) = &self.class.class_uri {
                id = Identifier::new(curi);
            }
        }
        if expand {
            Ok(id.to_uri(conv)?.into())
        } else {
            Ok(id.to_curie(conv)?.into())
        }
    }

    pub fn get_type_designator_value(
        &self,
        sv: &SchemaView,
        slot: &SlotDefinition,
        conv: &Converter,
    ) -> Result<Identifier, IdentifierError> {
        let range = slot.range.as_deref().unwrap_or("string");
        let types = sv.type_ancestors(&Identifier::new(range));
        if types.iter().any(|t| t == &Identifier::new("uri")) {
            self.get_uri(conv, false, true)
        } else if types.iter().any(|t| t == &Identifier::new("uriorcurie")) {
            self.get_uri(conv, false, false)
        } else if types.iter().any(|t| t == &Identifier::new("string")) {
            Ok(Identifier::Name(self.class.name.clone()))
        } else {
            self.get_uri(conv, false, false)
        }
    }

    pub fn get_accepted_type_designator_values(
        &self,
        sv: &SchemaView,
        slot: &SlotDefinition,
        conv: &Converter,
    ) -> Result<Vec<Identifier>, IdentifierError> {
        let mut vals = vec![
            self.get_uri(conv, true, true)?,
            self.get_uri(conv, false, true)?,
            self.get_uri(conv, true, false)?,
            self.get_uri(conv, false, false)?,
        ];
        vals.dedup();

        let range = slot.range.as_deref().unwrap_or("string");
        let types = sv.type_ancestors(&Identifier::new(range));
        if types
            .iter()
            .any(|t| t == &Identifier::new("uri") || t == &Identifier::new("uriorcurie"))
        {
            Ok(vals)
        } else if range == "string" || types.iter().any(|t| t == &Identifier::new("string")) {
            Ok(vec![Identifier::Name(self.class.name.clone())])
        } else {
            Ok(vals)
        }
    }
}

impl SchemaView {
    pub fn new() -> Self {
        SchemaView {
            schema_definitions: HashMap::new(),
            primary_schema: None,
            class_uri_index: HashMap::new(),
        }
    }

    pub fn add_schema(&mut self, schema: SchemaDefinition) -> Result<(), String> {
        let schema_uri = schema.id.clone();
        let conv = converter_from_schema(&schema);
        self.index_schema_classes(&schema_uri, &schema, &conv)
            .map_err(|e| format!("{:?}", e))?;
        self.schema_definitions
            .insert(schema_uri.to_string(), schema);
        if self.primary_schema.is_none() {
            self.primary_schema = Some(schema_uri.to_string());
        }
        Ok(())
    }

    fn index_schema_classes(
        &mut self,
        schema_uri: &str,
        schema: &SchemaDefinition,
        conv: &Converter,
    ) -> Result<(), IdentifierError> {
        let default_prefix = schema.default_prefix.as_deref().unwrap_or(&schema.name);
        for (class_name, class_def) in &schema.classes {
            let default_uri = Identifier::new(&format!("{}:{}", default_prefix, class_name))
                .to_uri(conv)
                .map(|u| u.0)
                .unwrap_or_else(|_| format!("{}/{}", schema.id.trim_end_matches('/'), class_name));

            if let Some(curi) = &class_def.class_uri {
                let explicit_uri = Identifier::new(curi).to_uri(conv)?.0;
                self.class_uri_index
                    .entry(explicit_uri.clone())
                    .or_insert_with(|| (schema_uri.to_string(), class_name.clone()));
                if explicit_uri != default_uri {
                    self.class_uri_index
                        .entry(default_uri.clone())
                        .or_insert_with(|| (schema_uri.to_string(), class_name.clone()));
                }
            } else {
                self.class_uri_index
                    .entry(default_uri)
                    .or_insert_with(|| (schema_uri.to_string(), class_name.clone()));
            }
        }
        Ok(())
    }

    pub fn get_unresolved_schemas(&self) -> Vec<String> {
        // every schemadefinition has imports. check if an import is not in our list
        let mut unresolved = Vec::new();
        for (_name, schema) in &self.schema_definitions {
            for import in &schema.imports {
                let import_uri = curie2uri(import, &schema.prefixes);
                match import_uri {
                    Some(uri) => {
                        if !self.schema_definitions.contains_key(&uri) {
                            unresolved.push(uri);
                        }
                    }
                    None => {}
                }
            }
        }
        unresolved
    }

    pub fn get_class<'a>(
        &'a self,
        id: &Identifier,
        conv: &Converter,
    ) -> Result<Option<ClassView<'a>>, IdentifierError> {
        let index = &self.class_uri_index;
        match id {
            Identifier::Name(name) => {
                let primary = match &self.primary_schema {
                    Some(p) => p,
                    None => return Ok(None),
                };
                let schema = match self.schema_definitions.get(primary) {
                    Some(s) => s,
                    None => return Ok(None),
                };
                Ok(schema.classes.get(name).map(|c| ClassView::new(c, schema)))
            }
            Identifier::Curie(_) | Identifier::Uri(_) => {
                let target_uri = id.to_uri(conv)?;
                if let Some((schema_uri, class_name)) = index.get(&target_uri.0) {
                    if let Some(schema) = self.schema_definitions.get(schema_uri) {
                        if let Some(class) = schema.classes.get(class_name) {
                            return Ok(Some(ClassView::new(class, schema)));
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn get_type<'a>(&'a self, id: &Identifier) -> Option<&'a TypeDefinition> {
        let key = match id {
            Identifier::Uri(u) => &u.0,
            Identifier::Curie(c) => &c.0,
            Identifier::Name(n) => n,
        };
        for schema in self.schema_definitions.values() {
            if let Some(t) = schema.types.get(key) {
                return Some(t);
            }
        }
        None
    }

    pub fn type_ancestors(&self, type_name: &Identifier) -> Vec<Identifier> {
        let mut ancestors = Vec::new();
        let mut current = Some(type_name.clone());
        while let Some(name) = current {
            ancestors.push(name.clone());
            current = self
                .get_type(&name)
                .and_then(|t| t.typeof_.as_ref().map(|s| Identifier::new(s)));
        }
        ancestors
    }
}
