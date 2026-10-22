#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
# Prints relevant=true|false depending on whether the diff against $1 touches anything db_bench is built from.
# A failing diff exits nonzero rather than reading as "nothing relevant".
set -euo pipefail

base="$1"
# Read the whole list before matching: a pipe into `grep -q` lets grep exit early, and the still-writing producer
# then dies of SIGPIPE, which pipefail reports as "no match".
changed="$(git diff --name-only "$base" HEAD)"
if grep -E '^(include/|src/|libs/|infrastructure/|tools/db_bench/|\.github/actions/setup-vcpkg/|vcpkg\.json$|vcpkg-configuration\.json$|CMakeLists\.txt$|CMakePresets\.json$)' \
     <<<"$changed" >/dev/null; then
  echo "relevant=true"
else
  echo "relevant=false"
fi
