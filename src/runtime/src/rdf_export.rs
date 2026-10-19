//! Public RDF export API.
//!
//! Mirrors `rdf_import` on the output side. Takes an instance (or an
//! iterable of instances) plus a writer; streams RDF into the writer.

#![cfg(feature = "ttl")]

use std::io::{Result as IoResult, Write};

use linkml_meta::SchemaDefinition;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::Converter;

use crate::turtle::{
    write_nquads_counted, write_ntriples, write_ntriples_counted, write_trig_counted, write_turtle,
    write_turtle_counted, WriteOptions,
};
use crate::LinkMLInstance;

/// Write a single instance as Turtle. See [`write_turtle`] for what `options`
/// may route.
pub fn export_turtle<W: Write>(
    instance: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &WriteOptions,
) -> IoResult<()> {
    write_turtle(instance, sv, schema, conv, writer, options)
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
    options: &WriteOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    options.validate("Turtle", true)?;
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_turtle_counted(&instance, sv, schema, conv, writer, options, &mut counter)?;
    }
    Ok(())
}

/// Write a single instance as N-Triples. See [`write_ntriples`] for what
/// `options` may route.
pub fn export_ntriples<W: Write>(
    instance: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &WriteOptions,
) -> IoResult<()> {
    write_ntriples(instance, sv, schema, conv, writer, options)
}

/// Write a stream of instances as N-Triples.
pub fn export_ntriples_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &WriteOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    options.validate("N-Triples", true)?;
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_ntriples_counted(&instance, sv, schema, conv, writer, options, &mut counter)?;
    }
    Ok(())
}

/// Write a stream of instances as N-Quads, each partition into the graph
/// `options` routes it to. See [`crate::turtle::write_nquads`].
pub fn export_nquads_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &WriteOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    options.validate("N-Quads", false)?;
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_nquads_counted(&instance, sv, schema, conv, writer, options, &mut counter)?;
    }
    Ok(())
}

/// Write a stream of instances as TriG, each partition into the graph
/// `options` routes it to. See [`crate::turtle::write_trig`].
pub fn export_trig_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &WriteOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    options.validate("TriG", false)?;
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_trig_counted(&instance, sv, schema, conv, writer, options, &mut counter)?;
    }
    Ok(())
}
