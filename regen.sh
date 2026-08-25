#!/bin/bash
# Regenerate the `linkml_meta` crate from meta.yaml with the linkml rust
# generator, and record which generator revision produced it.
#
#   ./regen.sh            regenerate src/metamodel/ in place
#   ./regen.sh --check     regenerate into a temp dir and diff; no writes
#
# `--check` is the reproducibility gate: the committed crate must be exactly
# what the generator in $LINKML_DIR emits. A non-empty diff means either the
# checkout moved or something was hand-edited into generated code. Both are
# worth knowing about before a regen silently reverts one of them.
#
# The generator lives in a linkml checkout, not in this repo. Point at it with
# LINKML_DIR (default `../linkml`) on a branch carrying the rust generator, and
# recreate its venv with `cd $LINKML_DIR && uv sync` if it is missing.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The default sibling checkout is a sibling of the *main* working tree, so this
# still finds it when run from a git worktree nested inside the repo.
git_common=$(cd "$REPO_ROOT" && git rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)
if [[ -n $git_common && -d $git_common ]]; then
  main_checkout=$(cd "$git_common/.." && pwd)
else
  main_checkout=$REPO_ROOT
fi
LINKML_DIR=${LINKML_DIR:-$main_checkout/../linkml}
META_YAML=$REPO_ROOT/src/schemaview/tests/data/meta.yaml
CRATE_DIR=$REPO_ROOT/src/metamodel
PROVENANCE=$CRATE_DIR/GENERATED_FROM

check=0
if [[ ${1:-} == "--check" ]]; then
  check=1
elif [[ $# -gt 0 ]]; then
  echo "usage: $0 [--check]" >&2
  exit 2
fi

if [[ ! -d $LINKML_DIR ]]; then
  echo "no linkml checkout at $LINKML_DIR (override with LINKML_DIR=...)" >&2
  exit 1
fi
LINKML_DIR=$(cd "$LINKML_DIR" && pwd)

# The revision that produced this output, and — more useful when the output
# does not match — which generator commits are not upstream yet. Anything
# listed here has to be merged and this file re-stamped before another machine
# can reproduce the crate.
revision=$(git -C "$LINKML_DIR" rev-parse HEAD)
described=$(git -C "$LINKML_DIR" describe --always --dirty)
unmerged=$(git -C "$LINKML_DIR" log --oneline linkml/main..HEAD 2>/dev/null || true)

out_dir=$CRATE_DIR
tmp_dir=
if [[ $check -eq 1 ]]; then
  tmp_dir=$(mktemp -d)
  # shellcheck disable=SC2064
  trap "rm -rf '$tmp_dir'" EXIT
  out_dir=$tmp_dir
fi

echo "Generating linkml_meta from $META_YAML"
echo "  generator: $LINKML_DIR @ $described"
(cd "$LINKML_DIR" && uv run gen-rust "$META_YAML" \
  --output "$out_dir" \
  --force --serde -n linkml_meta --stacktrace)

if [[ $check -eq 1 ]]; then
  # The committed crate is formatted; format the fresh output the same way
  # before comparing, or every line looks changed.
  find "$out_dir" -name '*.rs' -print0 | xargs -0 rustfmt --edition 2021
  echo "Diffing against $CRATE_DIR ..."
  # Only the generated sources and Cargo.toml are compared; a --check run
  # never touches GENERATED_FROM, so exclude it. `*~` excludes the editor
  # backup that is committed next to Cargo.toml.
  if diff -r --exclude=GENERATED_FROM --exclude=target --exclude='*~' \
    "$CRATE_DIR" "$out_dir"; then
    echo "OK: committed crate reproduces from $described"
  else
    echo
    echo "DRIFT: the committed crate is not what this generator emits." >&2
    echo "Either \$LINKML_DIR is on a different revision than GENERATED_FROM," >&2
    echo "or generated code was edited by hand." >&2
    exit 1
  fi
  exit 0
fi

{
  echo "# Provenance of the generated linkml_meta crate. Written by regen.sh."
  echo "# Do not edit by hand."
  echo "generator_repo = $(git -C "$LINKML_DIR" remote get-url origin 2>/dev/null || echo unknown)"
  echo "generator_revision = $revision"
  echo "generator_describe = $described"
  echo "meta_yaml = src/schemaview/tests/data/meta.yaml"
  echo
  if [[ -n $unmerged ]]; then
    echo "# Generator commits NOT in linkml/main. Until these are merged, this"
    echo "# crate cannot be reproduced from upstream linkml alone."
    while IFS= read -r line; do echo "# $line"; done <<<"$unmerged"
  else
    echo "# Every generator commit is in linkml/main; reproducible from upstream."
  fi
} >"$PROVENANCE"

echo "Wrote $PROVENANCE"
echo "Running cargo fmt over the workspace..."
(cd "$REPO_ROOT" && cargo fmt --all)
