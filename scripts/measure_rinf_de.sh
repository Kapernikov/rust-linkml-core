#!/usr/bin/env bash
# Measure RINF Germany .nt import via the existing `linkml-convert` CLI.
# Run TWICE during this MR:
#   1) before any algorithm change (captures pre-refactor baseline)
#   2) after Task 10 (captures post-refactor streaming behaviour)
# Same CLI, same args, same input → apples to apples.
#
# Override defaults with env vars:
#   PROCMON       path to procmon binary
#   NT_FILE       path to de_1080.nt
#   SCHEMA        path to the entry schema (rinf_subset.yaml)
#   LABEL         tag for the run (default: timestamped)
#   OUT_DIR       where to write outputs (default: target/rinf-measure/$LABEL)
set -euo pipefail

PROCMON="${PROCMON:-/tmp/procmon/result/bin/procmon}"
NT_FILE="${NT_FILE:-/home/kervel/projects/asset360/lml/de_1080.nt}"
SCHEMA="${SCHEMA:-/home/kervel/projects/asset360/consolidator-server/components/py/asset360-model/asset360_model/schemas/rinf/repository/v1.0.0/rinf_subset.yaml}"
LABEL="${LABEL:-$(date +%Y%m%d-%H%M%S)}"
OUT_DIR="${OUT_DIR:-target/rinf-measure/$LABEL}"
DISK_GRAPH="${DISK_GRAPH:-}"   # non-empty path → use disk-backed store at this path

mkdir -p "$OUT_DIR"

# Rebuild linkml-convert against the current source.
if [ -n "$DISK_GRAPH" ]; then
    cargo build -p linkml_tools --release --features disk_graph 2>&1 | tail -3
else
    cargo build -p linkml_tools --release 2>&1 | tail -3
fi

BIN="target/release/linkml-convert"
if [ ! -x "$BIN" ]; then
    echo "linkml-convert not found at $BIN" >&2
    exit 1
fi

# Root classes used for the RINF dataset. Match src/runtime/tests/rinf_import_test.rs.
ROOT_CLASSES=(
    OperationalPoint
    SectionOfLine
    RunningTrack
    Siding
    Tunnel
    ContactLineSystem
    TrainDetectionSystem
    PlatformEdge
    OrganisationRole
    LinearPositioningSystem
    ETCS
)
CLASS_ARGS=()
for c in "${ROOT_CLASSES[@]}"; do
    CLASS_ARGS+=(--class "$c")
done

echo "=== run: $LABEL ==="
echo "binary:    $BIN"
echo "nt:        $NT_FILE"
echo "schema:    $SCHEMA"
echo "output:    $OUT_DIR/output.json"
echo "procmon:   $PROCMON"

CMD=("$BIN" "$SCHEMA" "$NT_FILE" --from ntriples --to json --output "$OUT_DIR/output.json" "${CLASS_ARGS[@]}")
if [ -n "$DISK_GRAPH" ]; then
    rm -rf "$DISK_GRAPH"
    mkdir -p "$DISK_GRAPH"
    CMD+=(--disk-graph "$DISK_GRAPH")
    echo "disk-graph: $DISK_GRAPH"
fi

if [ -x "$PROCMON" ]; then
    "$PROCMON" --output "$OUT_DIR/procmon.log" -- "${CMD[@]}" 2>&1 | tee "$OUT_DIR/stdout.log"
else
    echo "procmon not available; falling back to /usr/bin/time -v" >&2
    /usr/bin/time -v "${CMD[@]}" 2>&1 | tee "$OUT_DIR/stdout.log"
fi

# Canonicalize the JSON output so two runs (with different HashMap iteration
# orders) produce byte-identical files. We sort the top-level array of
# instances by their serialized-with-sorted-keys form. Writes side-by-side
# `output.canonical.json` and keeps the raw `output.json` for inspection.
echo "Canonicalizing JSON..."
python3 - "$OUT_DIR/output.json" "$OUT_DIR/output.canonical.json" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
with open(src) as f:
    data = json.load(f)
def canon(v):
    if isinstance(v, dict):
        return {k: canon(v[k]) for k in sorted(v)}
    if isinstance(v, list):
        return [canon(x) for x in v]
    return v
data = canon(data)
if isinstance(data, list):
    # Top-level array: sort by serialized form for stable ordering.
    data.sort(key=lambda x: json.dumps(x, sort_keys=True))
with open(dst, 'w') as f:
    json.dump(data, f, sort_keys=True, indent=2)
print(f"Canonical JSON written to {dst}")
PY

ls -la "$OUT_DIR/"
echo "Done. Results in $OUT_DIR"
