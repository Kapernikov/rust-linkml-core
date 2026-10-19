//! Public RDF export API.
//!
//! Mirrors `rdf_import` on the output side. Takes an instance (or an
//! iterable of instances) plus a writer; streams RDF into the writer.

#![cfg(feature = "ttl")]

use std::io::{Result as IoResult, Write};

use linkml_meta::SchemaDefinition;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::Converter;

use crate::schema_rdf::TypeRegime;
use crate::turtle::{
    write_nquads, write_nquads_counted, write_ntriples, write_ntriples_counted, write_trig,
    write_trig_counted, write_turtle, write_turtle_counted, QuadOptions, TurtleOptions,
};
use crate::LinkMLInstance;

/// Options for the flat (triple) exports.
///
/// Built with [`ExportOptions::default`] and the `with_*` methods; the struct
/// is `#[non_exhaustive]` so new options do not break callers.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ExportOptions {
    /// Use skolem IRIs instead of blank nodes.
    pub skolem: bool,
    /// Which type triples to write. The default is
    /// [`TypeRegime::ASSERTED`].
    pub regime: TypeRegime,
}

impl ExportOptions {
    /// The same options with skolem IRIs on or off.
    pub fn with_skolem(mut self, skolem: bool) -> Self {
        self.skolem = skolem;
        self
    }

    /// The same options under another type regime.
    pub fn with_regime(mut self, regime: TypeRegime) -> Self {
        self.regime = regime;
        self
    }
}

impl From<&ExportOptions> for TurtleOptions {
    fn from(o: &ExportOptions) -> Self {
        TurtleOptions::default()
            .with_skolem(o.skolem)
            .with_regime(o.regime)
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
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_turtle_counted(
            &instance,
            sv,
            schema,
            conv,
            writer,
            (&options).into(),
            &mut counter,
        )?;
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
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_ntriples_counted(
            &instance,
            sv,
            schema,
            conv,
            writer,
            (&options).into(),
            &mut counter,
        )?;
    }
    Ok(())
}

/// Write a single instance as N-Quads, one named graph per type partition.
pub fn export_nquads<W: Write>(
    instance: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &QuadOptions,
) -> IoResult<()> {
    write_nquads(instance, sv, schema, conv, writer, options)
}

/// Write a stream of instances as N-Quads.
pub fn export_nquads_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &QuadOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_nquads_counted(&instance, sv, schema, conv, writer, options, &mut counter)?;
    }
    Ok(())
}

/// Write a single instance as TriG, one named graph per type partition.
pub fn export_trig<W: Write>(
    instance: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &QuadOptions,
) -> IoResult<()> {
    write_trig(instance, sv, schema, conv, writer, options)
}

/// Write a stream of instances as TriG.
pub fn export_trig_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: &QuadOptions,
) -> IoResult<()>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>,
{
    // One blank node counter for the whole stream: labels never repeat.
    let mut counter = 0;
    for instance in instances {
        write_trig_counted(&instance, sv, schema, conv, writer, options, &mut counter)?;
    }
    Ok(())
}
