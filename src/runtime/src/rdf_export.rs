//! Public RDF export API.
//!
//! Mirrors `rdf_import` on the output side. Takes an instance (or an
//! iterable of instances) plus a writer; streams RDF into the writer.
//! [`RdfStreamWriter`] is the primitive for writing many instances into one
//! stream; the `export_turtle_many` / `export_ntriples_many` functions are conveniences
//! over it. Quads are written only through the stream writer.

#![cfg(feature = "ttl")]

use std::collections::BTreeSet;
use std::io::{Result as IoResult, Write};

use linkml_meta::SchemaDefinition;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::Converter;

use crate::turtle::{
    write_nquads_counted, write_ntriples, write_ntriples_counted, write_trig_counted, write_turtle,
    write_turtle_counted, WriteOptions,
};
use crate::LinkMLInstance;

/// The serialization an [`RdfStreamWriter`] produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RdfOutputFormat {
    /// Turtle. A flat format: every partition must be routed to the default
    /// graph.
    Turtle,
    /// N-Triples. A flat format, as [`Turtle`](Self::Turtle).
    NTriples,
    /// N-Quads: each partition goes into the graph the options route it to.
    NQuads,
    /// TriG: each partition goes into the graph the options route it to.
    TriG,
}

impl RdfOutputFormat {
    fn name(self) -> &'static str {
        match self {
            RdfOutputFormat::Turtle => "Turtle",
            RdfOutputFormat::NTriples => "N-Triples",
            RdfOutputFormat::NQuads => "N-Quads",
            RdfOutputFormat::TriG => "TriG",
        }
    }

    /// Whether the format cannot carry a graph name.
    fn is_flat(self) -> bool {
        matches!(self, RdfOutputFormat::Turtle | RdfOutputFormat::NTriples)
    }
}

/// Writes any number of instances into one RDF stream.
///
/// All instances share one blank node counter, so blank node labels never
/// repeat within the stream. Each instance is serialized independently and
/// appended. Memory use is
/// bounded by one instance.
///
/// Turtle and TriG declare each `@prefix` once: the first time an instance in
/// the stream uses it, just before that instance's triples (both formats
/// allow `@prefix` between statements).
pub struct RdfStreamWriter<'a, W: Write> {
    writer: W,
    format: RdfOutputFormat,
    sv: &'a SchemaView,
    schema: &'a SchemaDefinition,
    conv: &'a Converter,
    options: WriteOptions,
    counter: usize,
    declared_prefixes: BTreeSet<String>,
}

impl<'a, W: Write> RdfStreamWriter<'a, W> {
    /// Start a stream of `format` into `writer`.
    ///
    /// Validates `options` for the format up front, with the errors the
    /// single-instance writers give: an empty `partition_graphs` is an
    /// `InvalidInput` error, and so is a partition routed to a named graph
    /// when the format cannot carry one (Turtle, N-Triples).
    pub fn new(
        writer: W,
        format: RdfOutputFormat,
        sv: &'a SchemaView,
        schema: &'a SchemaDefinition,
        conv: &'a Converter,
        options: &WriteOptions,
    ) -> IoResult<Self> {
        options.validate(format.name(), format.is_flat())?;
        Ok(Self {
            writer,
            format,
            sv,
            schema,
            conv,
            options: options.clone(),
            counter: 0,
            declared_prefixes: BTreeSet::new(),
        })
    }

    /// Write one instance. Blank node labels never repeat within the stream.
    pub fn write(&mut self, instance: &LinkMLInstance) -> IoResult<()> {
        let (sv, schema, conv, options) = (self.sv, self.schema, self.conv, &self.options);
        let (w, counter, declared) = (
            &mut self.writer,
            &mut self.counter,
            &mut self.declared_prefixes,
        );
        match self.format {
            RdfOutputFormat::Turtle => {
                write_turtle_counted(instance, sv, schema, conv, w, options, counter, declared)
            }
            RdfOutputFormat::NTriples => {
                write_ntriples_counted(instance, sv, schema, conv, w, options, counter)
            }
            RdfOutputFormat::NQuads => {
                write_nquads_counted(instance, sv, schema, conv, w, options, counter)
            }
            RdfOutputFormat::TriG => {
                write_trig_counted(instance, sv, schema, conv, w, options, counter, declared)
            }
        }
    }

    /// Flush and return the inner writer.
    pub fn finish(mut self) -> IoResult<W> {
        self.writer.flush()?;
        Ok(self.writer)
    }
}

/// Write every instance of `instances` through one [`RdfStreamWriter`].
fn export_many<W, I>(
    format: RdfOutputFormat,
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
    let mut stream = RdfStreamWriter::new(writer, format, sv, schema, conv, options)?;
    for instance in instances {
        stream.write(&instance)?;
    }
    stream.finish()?;
    Ok(())
}

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

/// Write a stream of instances as Turtle. A convenience over
/// [`RdfStreamWriter`] with [`RdfOutputFormat::Turtle`]; use the writer
/// directly to interleave other work between instances.
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
    export_many(
        RdfOutputFormat::Turtle,
        instances,
        sv,
        schema,
        conv,
        writer,
        options,
    )
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

/// Write a stream of instances as N-Triples. A convenience over
/// [`RdfStreamWriter`] with [`RdfOutputFormat::NTriples`].
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
    export_many(
        RdfOutputFormat::NTriples,
        instances,
        sv,
        schema,
        conv,
        writer,
        options,
    )
}
