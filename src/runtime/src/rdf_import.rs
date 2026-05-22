//! Public RDF import API.
//!
//! The top-level entry points (`import_turtle`, `import_ntriples`) plus
//! the `RdfStream` iterator type that callers consume.
//!
//! Backend selection is runtime-driven via `ImportOptions::disk_path`:
//! `None` → in-memory store; `Some(path)` → fjall-backed disk store
//! (requires the `disk_graph` cargo feature).

#![cfg(feature = "ttl")]

use std::cell::RefCell;
use std::io::Read;
use std::path::PathBuf;
use std::rc::Rc;

use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::Converter;

use crate::rdf_import_store::RdfImportStore;
use crate::rdf_streaming::{import_owned_store_streaming, OwnedImportStream};
use crate::turtle_import::{ImportError, RdfFormat};
use crate::{LinkMLInstance, ValidationResult};

/// Options accepted by `import_turtle` / `import_ntriples`.
#[derive(Debug, Default, Clone)]
pub struct ImportOptions {
    /// If set, build a disk-backed store at this path instead of an
    /// in-memory one. Requires the `disk_graph` cargo feature.
    pub disk_path: Option<PathBuf>,
    /// If true, the first emitted warning aborts the import as an error.
    pub strict: bool,
}

/// Internal backend dispatch. The set of variants is feature-gated so
/// `disk_graph`-off builds don't drag in fjall.
enum RdfStreamInner {
    Memory(OwnedImportStream<RdfImportStore>),
    #[cfg(feature = "disk_graph")]
    Disk(OwnedImportStream<crate::rdf_import_store_disk::DiskRdfImportStore>),
}

/// Streaming iterator over harvested LinkML instances.
///
/// Yields `(class_name, LinkMLInstance)` pairs. After iteration completes,
/// call `unconsumed_subjects()` (in-memory backend only) to inspect what
/// the harvest didn't reach. At any point during or after iteration,
/// call `pop_warnings()` to drain the accumulated diagnostic warnings.
pub struct RdfStream {
    inner: RdfStreamInner,
    /// Shared warning buffer. Same `Rc` clone as the one inside the
    /// underlying `HarvestContext`, so warnings pushed by the harvest are
    /// visible here.
    warnings: Rc<RefCell<Vec<ValidationResult>>>,
    /// Tracks whether iteration has completed at least once. Some methods
    /// (like `unconsumed_subjects`) are only meaningful after exhaustion.
    finished: bool,
}

impl RdfStream {
    /// Drain accumulated warnings. Subsequent calls return only new
    /// warnings emitted since the previous drain.
    pub fn pop_warnings(&mut self) -> Vec<ValidationResult> {
        std::mem::take(&mut *self.warnings.borrow_mut())
    }

    /// Number of warnings currently buffered (does not drain).
    pub fn warning_count(&self) -> usize {
        self.warnings.borrow().len()
    }

    /// Borrow a clone of the shared warnings handle. Useful when the
    /// caller wants to consume the stream as an iterator (which moves
    /// `self`) but still drain warnings afterwards via the cloned `Rc`.
    pub fn warnings_handle(&self) -> Rc<RefCell<Vec<ValidationResult>>> {
        self.warnings.clone()
    }

    /// Subjects present in the source RDF that the harvest never visited.
    /// Returns `None` if:
    ///   - the disk backend is in use (computing this on disk requires a
    ///     full SPO scan plus per-subject prefix re-scans, deliberately
    ///     skipped — use the in-memory backend for schema-debugging);
    ///   - iteration has not completed yet (the consumed-subject set is
    ///     only complete at exhaustion).
    pub fn unconsumed_subjects(&self) -> Option<Vec<(String, usize)>> {
        if !self.finished {
            return None;
        }
        match &self.inner {
            RdfStreamInner::Memory(stream) => Some(stream.store().unconsumed_subjects()),
            #[cfg(feature = "disk_graph")]
            RdfStreamInner::Disk(_) => None,
        }
    }
}

impl Iterator for RdfStream {
    type Item = Result<(String, LinkMLInstance), ImportError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match &mut self.inner {
            RdfStreamInner::Memory(s) => s.next(),
            #[cfg(feature = "disk_graph")]
            RdfStreamInner::Disk(s) => s.next(),
        };
        if next.is_none() {
            self.finished = true;
        }
        next
    }
}

/// Build an `RdfStream` for the given format. Internal helper used by the
/// public `import_turtle` and `import_ntriples`.
fn build_stream<R: Read>(
    reader: R,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
    options: ImportOptions,
    format: RdfFormat,
) -> Result<RdfStream, ImportError> {
    let warnings: Rc<RefCell<Vec<ValidationResult>>> = Rc::new(RefCell::new(Vec::new()));

    let inner = match options.disk_path {
        Some(_path) => {
            #[cfg(feature = "disk_graph")]
            {
                use crate::rdf_import_store_disk::DiskRdfImportStore;
                let store = DiskRdfImportStore::from_rdf(reader, format, &_path)
                    .map_err(|e| ImportError::Parse(e.to_string()))?;
                let stream = import_owned_store_streaming(
                    store,
                    sv,
                    conv,
                    root_classes,
                    warnings.clone(),
                    options.strict,
                )?;
                RdfStreamInner::Disk(stream)
            }
            #[cfg(not(feature = "disk_graph"))]
            {
                let _ = (reader, sv, conv, root_classes, format);
                return Err(ImportError::Parse(
                    "disk_path requires the `disk_graph` cargo feature".to_string(),
                ));
            }
        }
        None => {
            let store = RdfImportStore::from_rdf(reader, format)?;
            let stream = import_owned_store_streaming(
                store,
                sv,
                conv,
                root_classes,
                warnings.clone(),
                options.strict,
            )?;
            RdfStreamInner::Memory(stream)
        }
    };

    Ok(RdfStream {
        inner,
        warnings,
        finished: false,
    })
}

/// Import RDF/Turtle into a streaming iterator of LinkML instances.
pub fn import_turtle<R: Read>(
    reader: R,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
    options: ImportOptions,
) -> Result<RdfStream, ImportError> {
    build_stream(reader, sv, conv, root_classes, options, RdfFormat::Turtle)
}

/// Import RDF/N-Triples into a streaming iterator of LinkML instances.
pub fn import_ntriples<R: Read>(
    reader: R,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
    options: ImportOptions,
) -> Result<RdfStream, ImportError> {
    build_stream(reader, sv, conv, root_classes, options, RdfFormat::NTriples)
}
