#!/usr/bin/env bash
# Run the showcase notebook end-to-end using papermill.
# Usage: ./notebooks/test_notebook.sh
#
# Creates a temporary venv, builds the Rust extension with maturin,
# and executes every cell in the notebook. Exits non-zero on failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv-notebook-test"

echo "=== Creating temporary venv in $VENV_DIR ==="
python3 -m venv "$VENV_DIR"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "=== Installing maturin, papermill, ipykernel ==="
pip install --quiet maturin papermill ipykernel

echo "=== Building linkml-runtime-rust with maturin develop ==="
maturin develop --manifest-path "$PROJECT_ROOT/src/python/Cargo.toml"

echo "=== Running showcase notebook ==="
python -m papermill \
    "$SCRIPT_DIR/showcase.ipynb" \
    /tmp/showcase_test_output.ipynb \
    --no-progress-bar

echo "=== All cells passed ==="

# Clean up
deactivate
rm -rf "$VENV_DIR"
echo "=== Cleaned up test venv ==="
