# RocksDB vcpkg overlay port

A vcpkg overlay replaces the whole upstream port, so every patch the port applies must live in this directory,
including the ones that are identical to upstream.

- **Forked from:** `microsoft/vcpkg` `ports/rocksdb` at version `11.8.1` (see `vcpkg.json`).
- **Activated by:** the root `CMakeLists.txt`, which appends this directory to `VCPKG_OVERLAY_PORTS` when
  `AVEVA_ROCKSDB_BUILD_DB_BENCH=ON`; it is deliberately not listed in the repository's `vcpkg-configuration.json`.
- **Features:** the port keeps upstream's feature set unchanged (including `tbb`, which this repository does not
  enable) so the overlay stays a small diff; only `db-bench` is AVEVA-specific.

## Patches

| Patch | Origin | Why it exists |
|-------|--------|---------------|
| `0001-fix-dependencies.patch` | Identical to the upstream vcpkg port (checked against `microsoft/vcpkg` master at 11.8.1) | Makes RocksDB's CMake use vcpkg's config packages for Snappy, lz4, zstd and the others instead of its own find modules. |
| `0002-fix-android.patch` | Identical to the upstream vcpkg port (same check) | Android API level guard for the `POSIX_FADV_*` placeholder macros. |
| `0004-install-db-bench-tool-lib.patch` | AVEVA only | Builds the non-`main` db_bench sources as an installed static library, `RocksDB::db_bench_tool`, so a wrapper can register the Azure filesystem plugin before it calls `rocksdb::db_bench_tool(argc, argv)`. Upstream links these sources straight into the `db_bench` executable. The patch is gated on `WITH_GFLAGS` only and deliberately not on `WITH_BENCHMARK_TOOLS`, which also builds benchmarks that need RocksDB's gtest and fail to link on Windows. The port's `db-bench` feature turns it on. |

## When to remove the overlay

Remove the whole overlay (and the `VCPKG_OVERLAY_PORTS` line in the root `CMakeLists.txt`) when the upstream port can provide
`RocksDB::db_bench_tool`, or when nothing in this repository still needs the `db-bench` feature
(`AVEVA_ROCKSDB_BUILD_DB_BENCH`). Until then, 0001 and 0002 cannot be deleted on their own: the portfile lists
them under `PATCHES`.

## Updating

When `builtin-baseline` in `vcpkg.json` moves, or you bump the RocksDB version, change these together; a partial
update either fails to download or silently builds a different source tree than the patches were written for:

1. Diff `$VCPKG_ROOT/ports/rocksdb` against this directory and bring over the upstream `portfile.cmake`
   changes, `vcpkg.json` (`version`, dependencies, features) and the upstream patches (`0001`, `0002`).
2. Set `REF "v${VERSION}"` and the `SHA512` of the new source archive. Take the SHA512 from the upstream port at the
   same version, or run the install once with `SHA512 0` and copy the hash vcpkg reports.
3. Re-apply `0004-install-db-bench-tool-lib.patch` (regenerate it with `git diff` against the new tag if the hunk
   at `if(WITH_BENCHMARK_TOOLS)` in RocksDB's `CMakeLists.txt` no longer applies).
4. Update the version recorded under "Forked from" above, then build with
   `cmake --preset LinuxRelease -DAVEVA_ROCKSDB_TESTS=OFF -DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON` (or the Windows preset).

The `db-bench` CI job (`.github/workflows/build-and-test.yml`) is the only job that builds the overlay's db_bench path.
It runs on every push to `main`, on manual `workflow_dispatch` runs with `build_db_bench` checked, and on pull
requests that `.github/scripts/detect-relevant-changes.sh` selects: those touching `include/`, `src/`, `libs/`,
`infrastructure/` (including this directory), `tools/db_bench/`, `tools/CMakeLists.txt`, the root `CMakeLists.txt`,
`CMakePresets.json`, `vcpkg.json`, `vcpkg-configuration.json`, `.github/actions/setup-vcpkg/`, the workflow itself or
the detection script. Other pull requests skip its build steps.

`0004` is AVEVA-only and has not been proposed upstream: no upstream RocksDB issue or pull request exists for it yet.
Upstream's `db_bench` links these sources straight into the executable, so the patch cannot be dropped until RocksDB
(or the vcpkg port) exposes a reusable `db_bench_tool` library. Add the link here if one is filed.