"""Utilities for pretty-printing LinkMLInstance trees.

This module provides a pure Python helper to render a :class:`LinkMLInstance`
as a human readable tree. It is intended for debugging and diagnostic use.
"""

from __future__ import annotations

from typing import Any, List

try:  # pragma: no cover - runtime optional during type checking
    from . import LinkMLInstance
except Exception:  # pragma: no cover - fallback when extension missing
    LinkMLInstance = Any  # type: ignore[misc]

__all__ = ["pretty_linkml_instance"]


def pretty_linkml_instance(value: "LinkMLInstance", prefix: str = '', nofirstline: bool = False) -> str:
    """Return a tree-style string representation of ``value``.

    Parameters
    ----------
    value: LinkMLInstance
        The value to render.
    indent: int, optional
        Starting indentation (number of spaces).
    """
    prefix
    
    if value.kind in ("map", "object"):
        txt = f"{prefix}* [{value.class_name}]\n" if not nofirstline else f"[{value.class_name}]\n"
        for key in value.keys():
            rval = value[key]
            if rval.kind == "scalar":
                txt += f"{prefix}  | {key}={rval.as_python()}\n"
            elif rval.kind == "list":
                txt += f"{prefix}  | {key}:\n{pretty_linkml_instance(rval, prefix + '  | ')}"
            else:
                pfx =  f"{prefix}  | " + ' ' * len(key)
                txt += f"{prefix}  | {key}: {pretty_linkml_instance(rval, pfx, nofirstline=True)}"
                txt += f"{prefix}  |\n"
    elif value.kind == "list":
        txt = ""
        for idx in range(len(value)):
            rval = value[idx]
            if rval.kind == "scalar":
                txt += f"{prefix} - {rval.as_python()}\n"
            else:
                txt += f"{prefix} - {pretty_linkml_instance(rval, prefix + '   ', nofirstline=True)}"
    elif value.kind == "scalar":
        txt = f"{prefix}{value.as_python()}\n"
    elif value.kind == "null":
        txt = f"{prefix}null\n"
    else:
        txt = f"[UNKNOWN KIND={value.kind}]\n"
        
    return txt
