//! Bridge from Python file-like objects (`IO[bytes]`) to Rust `std::io::Read`.
//!
//! `PyReader::read` calls into Python's `.read(chunk_size)` over the GIL,
//! buffering the returned bytes so the consumer can pull arbitrary slice
//! sizes. The chunk size is large (default 256 KB) so GIL acquisitions
//! happen once per chunk, not per slice — amortized away on multi-MB
//! inputs.

use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult};

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes};

/// Default chunk requested from the Python file-like on each refill.
const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Reader that pulls bytes from a Python file-like object.
pub struct PyReader {
    obj: PyObject,
    chunk_size: usize,
    buf: Vec<u8>,
    buf_pos: usize,
    eof: bool,
}

impl PyReader {
    pub fn new(obj: PyObject) -> Self {
        Self {
            obj,
            chunk_size: DEFAULT_CHUNK_SIZE,
            buf: Vec::new(),
            buf_pos: 0,
            eof: false,
        }
    }

    /// Pull the next chunk from Python's `.read()`. Populates `self.buf`
    /// and resets `buf_pos`. Sets `eof` if Python returned 0 bytes.
    fn refill(&mut self) -> IoResult<()> {
        if self.eof {
            self.buf.clear();
            self.buf_pos = 0;
            return Ok(());
        }
        let result: PyResult<Vec<u8>> = Python::with_gil(|py| {
            let bound = self.obj.bind(py);
            let res = bound.call_method1("read", (self.chunk_size,))?;
            // Accept both bytes and bytearray.
            if let Ok(b) = res.downcast::<PyBytes>() {
                Ok(b.as_bytes().to_vec())
            } else {
                let raw: Vec<u8> = res.extract()?;
                Ok(raw)
            }
        });
        match result {
            Ok(chunk) => {
                if chunk.is_empty() {
                    self.eof = true;
                    self.buf.clear();
                    self.buf_pos = 0;
                } else {
                    self.buf = chunk;
                    self.buf_pos = 0;
                }
                Ok(())
            }
            Err(e) => Err(IoError::new(
                ErrorKind::Other,
                format!("Python file-like read() failed: {e}"),
            )),
        }
    }
}

impl Read for PyReader {
    fn read(&mut self, dst: &mut [u8]) -> IoResult<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        // Drain any leftover from the previous chunk first.
        if self.buf_pos < self.buf.len() {
            let available = self.buf.len() - self.buf_pos;
            let to_copy = available.min(dst.len());
            dst[..to_copy].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + to_copy]);
            self.buf_pos += to_copy;
            return Ok(to_copy);
        }
        // Refill from Python.
        self.refill()?;
        if self.eof {
            return Ok(0);
        }
        // Now `buf` has data; recursively drain.
        let available = self.buf.len() - self.buf_pos;
        let to_copy = available.min(dst.len());
        dst[..to_copy].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + to_copy]);
        self.buf_pos += to_copy;
        Ok(to_copy)
    }
}
