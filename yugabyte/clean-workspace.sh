#!/bin/bash

# Remove generated workspace artifacts (test stores, sorted results, logs).
# Resolves paths relative to this script, so it works from any directory.

set -euo pipefail

readonly SCRIPT_DIR="${0%/*}"

for dir in store results-sorted logs; do
  target="$SCRIPT_DIR/$dir"
  if [[ -e "$target" ]]; then
    echo "Removing $target"
    rm -rf "$target"
  fi
done
