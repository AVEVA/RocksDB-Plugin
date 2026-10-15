# RocksDB vcpkg overlay port

A vcpkg overlay replaces the whole upstream port, so every patch the port applies must live in this directory,
including the ones that are identical to upstream.

- **Forked from:** `microsoft/vcpkg` `ports/rocksdb` at version `11.8.1` (see `vcpkg.json`).
''s `vcpkg-configuration.json`.

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

When `builtin-baseline` in `vcpkg.json` moves, diff `$VCPKG_ROOT/ports/rocksdb` against this directory, bring
over the upstream version, `SHA512` and patch changes, and re-apply `0004`. The opt-in `db-bench` CI job
(workflow_dispatch) is the only job that builds the overlay's db_bench path.
