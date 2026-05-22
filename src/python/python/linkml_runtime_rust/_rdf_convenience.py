"""Pure-Python convenience helpers around the Rust RDF entry points.

The Rust extension exposes `import_turtle`, `import_ntriples`,
`export_turtle`, and `export_ntriples`, all taking Python file-like
objects (`IO[bytes]`). These helpers wrap them for the two most common
call shapes: path-based and string-based.
"""
from __future__ import annotations

import io
import os
from typing import IO, Iterable, Optional, Union

from ._native import (
    import_ntriples as _import_ntriples,
    import_turtle as _import_turtle,
    export_ntriples as _export_ntriples,
    export_turtle as _export_turtle,
    RdfStream,
)

PathLike = Union[str, os.PathLike]
Instance = "LinkMLInstance"  # forward ref; defined in _native


# ─── Import side ───────────────────────────────────────────────────────────

def import_turtle_path(
    path: PathLike,
    schema_view,
    root_classes: list[str],
    *,
    disk_path: Optional[str] = None,
    strict: bool = False,
) -> RdfStream:
    """Open `path` (binary mode) and stream Turtle into an `RdfStream`.

    The file is consumed synchronously inside the constructor before this
    function returns, so closing it immediately is safe.
    """
    f = open(path, "rb")
    try:
        return _import_turtle(
            f,
            schema_view,
            root_classes,
            disk_path=disk_path,
            strict=strict,
        )
    finally:
        f.close()


def import_ntriples_path(
    path: PathLike,
    schema_view,
    root_classes: list[str],
    *,
    disk_path: Optional[str] = None,
    strict: bool = False,
) -> RdfStream:
    """Open `path` (binary mode) and stream N-Triples into an `RdfStream`."""
    f = open(path, "rb")
    try:
        return _import_ntriples(
            f,
            schema_view,
            root_classes,
            disk_path=disk_path,
            strict=strict,
        )
    finally:
        f.close()


def import_turtle_str(
    text: str,
    schema_view,
    root_classes: list[str],
    *,
    disk_path: Optional[str] = None,
    strict: bool = False,
) -> RdfStream:
    """Parse Turtle from an in-memory string."""
    return _import_turtle(
        io.BytesIO(text.encode("utf-8")),
        schema_view,
        root_classes,
        disk_path=disk_path,
        strict=strict,
    )


def import_ntriples_str(
    text: str,
    schema_view,
    root_classes: list[str],
    *,
    disk_path: Optional[str] = None,
    strict: bool = False,
) -> RdfStream:
    """Parse N-Triples from an in-memory string."""
    return _import_ntriples(
        io.BytesIO(text.encode("utf-8")),
        schema_view,
        root_classes,
        disk_path=disk_path,
        strict=strict,
    )


# ─── Export side ───────────────────────────────────────────────────────────

def export_turtle_path(
    instances,
    schema_view,
    path: PathLike,
    *,
    skolem: bool = False,
) -> None:
    """Write `instances` to `path` as Turtle. `instances` may be a single
    LinkMLInstance or any iterable of LinkMLInstance."""
    with open(path, "wb") as f:
        _export_turtle(instances, schema_view, f, skolem=skolem)


def export_ntriples_path(
    instances,
    schema_view,
    path: PathLike,
    *,
    skolem: bool = False,
) -> None:
    """Write `instances` to `path` as N-Triples."""
    with open(path, "wb") as f:
        _export_ntriples(instances, schema_view, f, skolem=skolem)


def export_turtle_str(
    instances,
    schema_view,
    *,
    skolem: bool = False,
) -> str:
    """Serialize `instances` as a Turtle string."""
    buf = io.BytesIO()
    _export_turtle(instances, schema_view, buf, skolem=skolem)
    return buf.getvalue().decode("utf-8")


def export_ntriples_str(
    instances,
    schema_view,
    *,
    skolem: bool = False,
) -> str:
    """Serialize `instances` as an N-Triples string."""
    buf = io.BytesIO()
    _export_ntriples(instances, schema_view, buf, skolem=skolem)
    return buf.getvalue().decode("utf-8")
