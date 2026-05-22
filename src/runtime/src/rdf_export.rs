//! Public RDF export API.
//!
//! Mirrors `rdf_import` on the output side. Takes an instance (or an
//! iterable of instances) plus a writer; streams RDF into the writer.

#![cfg(feature = "ttl")]

use std::io::{Result as IoResult, Write};

use linkml_meta::SchemaDefinition;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::Converter;

use crate::turtle::{write_ntriples, write_turtle, TurtleOptions};
use crate::LinkMLInstance;

/// Options for export.
#[derive(Debug, Default, Clone)]
pub struct ExportOptions {
    /// Use skolem IRIs instead of blank nodes.
    pub skolem: bool,
}

impl From<&ExportOptions> for TurtleOptions {
    fn from(o: &ExportOptions) -> Self {
        TurtleOptions { skolem: o.skolem }
    }
}

/// Write a single instance as Turtle.
pub fn export_turtle<W: Write>(
    instance: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: ExportOptions,
) -> IoResult<()> {
    write_turtle(instance, sv, schema, conv, writer, (&options).into())
}

/// Write a stream of instances as Turtle, one per call to the underlying
/// serializer. Useful for streaming-harvest output where the instance
/// iterator yields one at a time and the caller doesn't want to collect.
pub fn export_turtle_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: ExportOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    for instance in instances {
        write_turtle(&instance, sv, schema, conv, writer, (&options).into())?;
    }
    Ok(())
}

/// Write a single instance as N-Triples.
pub fn export_ntriples<W: Write>(
    instance: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: ExportOptions,
) -> IoResult<()> {
    write_ntriples(instance, sv, schema, conv, writer, (&options).into())
}

/// Write a stream of instances as N-Triples.
pub fn export_ntriples_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: ExportOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    for instance in instances {
        write_ntriples(&instance, sv, schema, conv, writer, (&options).into())?;
    }
    Ok(())
}
