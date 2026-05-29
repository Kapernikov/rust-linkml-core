#!/bin/bash
set -euo pipefail

# Run the generator from the linkml checkout via its uv-managed venv.
# Recreate the venv with `cd ../linkml && uv sync` if it is missing.
cd ../linkml
uv run gen-rust ../rust-linkml-core/src/schemaview/tests/data/meta.yaml \
  --output ../rust-linkml-core/src/metamodel/ \
  --force --serde -n linkml_meta --stacktrace

# Reformat the Rust workspace to clean up generated code
cd ../rust-linkml-core
echo "Running cargo fmt over the workspace..."
cargo fmt --all
