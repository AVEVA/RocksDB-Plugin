# `aveva_db_bench` — RocksDB `db_bench` with the AVEVA Azure plugin

`aveva_db_bench` is a thin wrapper around Facebook's
[`db_bench`](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools) tool.
The wrapper registers the AVEVA Azure Page Blob filesystem plugin with the
RocksDB `ObjectLibrary`. The wrapper does this **before** it calls
`rocksdb::db_bench_tool`. This lets `db_bench` send I/O through Azure Blob
Storage, the same way a production consumer of the plugin does.

This tool is a POC deliverable for work item **4983896**. See "Follow-up
work" below for the planned next steps.

## Build

The wrapper is off by default. To build it, use any preset and pass the CMake option:

```powershell
cmake --preset WindowsRelease -DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON
cmake --build build/WindowsRelease --config RelWithDebInfo --target aveva_db_bench
```

The `db-bench` vcpkg feature is enabled in all presets, so the db_bench_tool is
available whenever you set the option. See the root
[README.md](../../README.md#build) for preset details and how to configure manually.

This build uses an [overlay port](../../infrastructure/vcpkg-overlays/rocksdb).
The overlay keeps `WITH_BENCHMARK_TOOLS=OFF` and instead applies
`0004-install-db-bench-tool-lib.patch`, which exports `RocksDB::db_bench_tool` as
an installable static library that the wrapper links against.

## Run — local filesystem (smoke test)

Set no `AVEVA_DB_BENCH_*` variables to pass through to stock `db_bench`. Use
this mode to check the build before you point the tool at Azure.

```powershell
./build/WindowsRelease/tools/db_bench/RelWithDebInfo/db_bench.exe `
    --benchmarks=fillseq,readrandom `
    --num=100000
```

## Prerequisites for an Azure-backed run

Gather these items before you start.

- **Azure Storage Account (Page Blob support).** Get the endpoint from the
  Azure Portal, under Storage account → Endpoints.
- **Blob container name.** Any valid Azure container name.
- **Entra ID (Azure AD) Service Principal.** Get the Tenant ID, Client
  (App) ID, and Client Secret from an App Registration.
- **RBAC role.** Storage Blob Data Contributor.

Notes on the items above:

- Container names use lowercase letters, digits, and hyphens only, 3 to 63
  characters. Do not use dots or underscores. A name like `user.name` fails
  with `InvalidResourceName`.

- The plugin creates the container if it does not exist. Do not create the
  container yourself.

- Grant the service principal the **Storage Blob Data Contributor** role on
  the storage account or container. Without this role, every blob call
  returns `403 Forbidden`, even when auth succeeds.

The wrapper supports Service Principal auth today only. To add Managed
Identity or `ChainedCredentialInfo` support, follow the second
`Plugin::Register` overload declared in `Plugin.hpp` (implemented in
`src/Azure/Plugin.cpp`) as a model.

## Run — Azure backend

Set the plugin configuration through the environment:

```powershell
$env:AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL = "https://<account>.blob.core.windows.net"
$env:AZURE_STORAGE_ACCOUNT_NAME         = "<account>"            # matches the account label above.
$env:AVEVA_DB_BENCH_CONTAINER           = "<container>"          # lowercase, digits, hyphens only
$env:AVEVA_DB_BENCH_TENANT_ID           = "<tenant-guid>"
$env:AVEVA_DB_BENCH_CLIENT_ID           = "<service-principal-app-id>"
$env:AVEVA_DB_BENCH_CLIENT_SECRET       = "<service-principal-secret>"
```

Then pass `--fs_uri` and `--db`. **Important:** `--db` is not the full
`https://` URL. `--db` is `<account>+<container>/<db-subpath>`, the value
that `StorageAccount::UniquePrefix()` builds. The wrapper prints the exact
string to use on startup, under "Suggested flags". Copy that string:

```powershell
$exe = ".\build\WindowsRelease\tools\db_bench\RelWithDebInfo\db_bench.exe"

& $exe `
    --benchmarks=fillseq,readrandom `
    --num=10000 `
    --compression_type=zlib `
    --fs_uri="azblobfs$($env:AVEVA_DB_BENCH_CONTAINER)" `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db"
```

Always add `--compression_type=zlib`. This build enables only the `zlib`
vcpkg feature for `rocksdb` (see the root `vcpkg.json`). This build does not
link the `snappy` compression that `db_bench` uses by default. Without this
flag, `db_bench` exits with the error `Compression type Snappy is not linked
with the binary`.

### Confirm the Azure plugin registered

The wrapper prints this line to stderr before any benchmark runs. It prints
this as soon as `Plugin::Register` succeeds.

```
aveva_db_bench: Azure plugin registered.
  Suggested flags:
    --fs_uri=azblobfs<container>
    --db=<account>+<container>/<your-db-name>
```

If you instead see the line `AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL not set;
Azure plugin will NOT be registered. Falling through to stock db_bench.`,
you did not set the env vars above in that shell before you ran the tool.

Do not read the console output by hand to check this line. Capture stderr
and search it instead.

```powershell
$out = & $exe --benchmarks=fillseq --num=10 --compression_type=zlib `
    --fs_uri="azblobfs$($env:AVEVA_DB_BENCH_CONTAINER)" `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db" 2>&1
# Search the captured output for the registration message.
if ($out -match "Azure plugin registered") { "PLUGIN OK" } else { "PLUGIN NOT REGISTERED" }
```

This command prints `PLUGIN OK` when the search finds the message.

### Confirm the container exists

The plugin calls `CreateIfNotExists()` on the container as soon as
`Plugin::Register` runs. You do not need to create the container first. To
check the container state yourself, log in to the Azure CLI as the same
service principal. This step avoids tenant-mismatch auth errors against
your personal `az login`.

```powershell
az login --service-principal -u $env:AVEVA_DB_BENCH_CLIENT_ID -p $env:AVEVA_DB_BENCH_CLIENT_SECRET --tenant $env:AVEVA_DB_BENCH_TENANT_ID
# Check the container state.
az storage container exists --account-name $env:AZURE_STORAGE_ACCOUNT_NAME --name $env:AVEVA_DB_BENCH_CONTAINER --auth-mode login
az login   # Switch back to your own identity when done.
```

The last command restores your personal Azure CLI identity.

### Check for a real Azure round-trip

Run `fillseq`. Then reopen the same `--db` path with `readrandom`, in a
**separate command**, with `--use_existing_db=1`. A new process can find the
keys only if it downloads them back from Azure.

```powershell
& $exe --benchmarks=readrandom --num=10000 --use_existing_db=1 --compression_type=zlib `
    --fs_uri="azblobfs$($env:AVEVA_DB_BENCH_CONTAINER)" `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db"
```
Check the `readrandom` result line for `(10000 of 10000 found)`.

## Attaching a secondary cache

The wrapper registers two AVEVA `rocksdb::SecondaryCache` backends with the
`ObjectLibrary`, so you can select one with db_bench's own
`--secondary_cache_uri` flag, the same way `--fs_uri` selects the Azure
plugin:

- `aveva-file-based-secondary-cache://` — `FileBasedCompressedSecondaryCache`
  (one file per entry, on local disk).
- `aveva-cachelib-secondary-cache://` — `CacheLibSecondaryCache`, backed by
  Facebook CacheLib's Navy engine. Only registered in builds configured with
  `-DAVEVA_ROCKSDB_WITH_CACHELIB=ON` (Linux x64 only; the CMake build fails
  fast with a `FATAL_ERROR` if you force it on elsewhere).

Both backends require a cache directory. Set these before launching:

```powershell
$env:AVEVA_DB_BENCH_SECONDARY_CACHE_DIR          = "C:\temp\secondary-cache"
$env:AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB  = "512"   # optional, default 512
```

Then add `--secondary_cache_uri` alongside your usual flags:

```powershell
& $exe --benchmarks=fillrandom,readrandom --num=200000 --value_size=1024 `
    --compression_type=zlib --statistics=1 --histogram=1 `
    --fs_uri="azblobfs$($env:AVEVA_DB_BENCH_CONTAINER)" `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db" `
    --secondary_cache_uri="aveva-file-based-secondary-cache://"
```

On CacheLib builds, swap the URI for `aveva-cachelib-secondary-cache://` and
optionally tune Navy through its own env vars (all optional; defaults shown):

```
AVEVA_DB_BENCH_CACHELIB_REGION_SIZE_BYTES                   (4194304)
AVEVA_DB_BENCH_CACHELIB_CLEAN_REGIONS                       (1)
AVEVA_DB_BENCH_CACHELIB_BLOCK_SIZE                          (4096)
AVEVA_DB_BENCH_CACHELIB_READER_THREADS                      (4)
AVEVA_DB_BENCH_CACHELIB_WRITER_THREADS                      (4)
AVEVA_DB_BENCH_CACHELIB_MAX_PARCEL_MEMORY_MB                (32)
AVEVA_DB_BENCH_CACHELIB_EVICTION_POLICY                     (fifo|lru, default fifo)
AVEVA_DB_BENCH_CACHELIB_DATA_CHECKSUM                       (0|1, default 1)
AVEVA_DB_BENCH_CACHELIB_ADMISSION_WRITE_RATE_BYTES_PER_SEC  (unset = admit everything)
AVEVA_DB_BENCH_CACHELIB_LOG_LEVEL                           (WARN)
AVEVA_DB_BENCH_CACHELIB_FILE_NAME                           (navy.cache)
AVEVA_DB_BENCH_CACHELIB_LOG_COUNTERS                        (0|1, default 1)
```

With `LOG_COUNTERS` on, closing the cache prints one `CACHELIB_COUNTER name=value`
line per Navy counter (e.g. `navy_bc_lookups`, `navy_bc_succ_lookups`, `navy_bc_inserts`,
`navy_bc_used_size_bytes`, and `navy_device_bytes_written` for SSD wear). The cache is
reset on every open, so the totals cover a single db_bench invocation. Counter
names vary by Navy version; list them with `grep CACHELIB_COUNTER <log>`.

The wrapper's stderr banner echoes the exact URIs and env vars available in
your build, so you don't need to remember this list.

### Known caveat: `aveva-file-based-secondary-cache` under-reports hits

`FileBasedCompressedSecondaryCache::Lookup()` currently passes
`rocksdb::CacheTier::kNonVolatileBlockTier` to RocksDB's `create_cb`, which
RocksDB's block-cache helpers reject. The lookup is treated as a miss at the
RocksDB layer even when the on-disk entry is valid, so
`rocksdb.secondary.cache.hits` (and friends) stay at zero for this backend
regardless of actual cache behavior. This is a known, tracked-in-tests issue
(`SecondaryCacheRocksDbTests.ReadsAreCorrectAndServedFromSecondaryCache`
skips the assertion for this backend) — not a benchmark artifact. Do not
use `--statistics=1`'s `SECONDARY_CACHE_*` tickers to judge this backend's
effectiveness until it's fixed; the CacheLib backend does not have this
issue (it round-trips the real tier through its own record header).

## Stats and histograms

Add these flags to get latency percentiles and RocksDB's internal counters.

```powershell
& $exe --benchmarks=fillrandom,readrandom --num=50000 --value_size=1024 `
    --compression_type=zlib --statistics=1 --histogram=1 `
    --fs_uri="azblobfs$($env:AVEVA_DB_BENCH_CONTAINER)" `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db" `
    *>&1 | Tee-Object -FilePath C:\temp\dbbench_output.txt
```

- `--histogram=1` adds per-benchmark P50/P95/P99/P100 latency to the summary line. Without this flag, `db_bench` reports only the average.
- `--statistics=1` prints RocksDB's internal `Statistics` counters at the end. These counters include cache hits and misses, compaction activity, and per-op latency histograms such as `rocksdb.db.write.micros`.
- Use `Tee-Object`/`*>&1` to save a local copy. Do not rely on `--report_file` for this (see the gotcha below).

### `--report_file` gotcha

`--report_file` has no effect unless you also set
`--report_interval_seconds` above 0. `db_bench --help` documents this rule,
but the rule is easy to miss.

When you set `--fs_uri`, the periodic reporter writes through `FLAGS_env`.
`FLAGS_env` becomes the Azure plugin's Env for the whole process. So the
report file becomes a blob inside your container, not a local file on disk.
For Azure runs, pipe console output to a local file with `Tee-Object`
instead.

### How to read the output

```
fillrandom   :  5234.234 micros/op   191 ops/sec   26.170 seconds   50000 operations;   0.2 MB/s
```
- **`micros/op`** is the average latency per operation. A lower number is
  better.
- **`ops/sec`** is the throughput. A higher number is better.
- **`(N of M found)`**, on `readrandom`, should read `M of M`. A lower count
  usually means the read key range did not match the keys you wrote.

Check these `--statistics=1` counters:

- `rocksdb.db.write.micros` — the end-to-end write latency. Compare the P50
  value here against a local-disk baseline run (see below). The
  difference is the Azure round-trip cost per write.

- `rocksdb.db.get.micros` — the read latency. This value often stays low
  even against Azure. RocksDB's in-memory block cache and memtable serve
  most reads, unless the active data set is larger than the cache.

- `rocksdb.manifest.file.sync.micros` — the cost of MANIFEST metadata
  syncs. This event is infrequent, but it is also network-bound against
  Azure.

- A counter that shows `COUNT: 0 SUM: 0` means your benchmark run did not
  use that code path. For example, a short `fillseq`-only run shows no
  compaction stats. This result is not an error.

## Where to look for performance improvements

Compare an Azure-backed run against an equivalent local-disk run with the
same flags.

```powershell
# Baseline: local disk.
& $exe --benchmarks=fillrandom,readrandom --num=200000 --value_size=1024 `
    --compression_type=zlib --statistics=1 --histogram=1 --db=C:\temp\baseline

# Plugin: Azure-backed.
& $exe --benchmarks=fillrandom,readrandom --num=200000 --value_size=1024 `
    --compression_type=zlib --statistics=1 --histogram=1 `
    --fs_uri="azblobfs$($env:AVEVA_DB_BENCH_CONTAINER)" `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db"
```

Compare the two output files side by side.

Check these signals, in priority order, for follow-up investigation:

1. **`rocksdb.db.write.micros` P50/P99, against the local baseline.** Manual runs show the plugin's per-write overhead near 40-50 ms median, against sub-millisecond on local disk. Network round-trips to Azure cause most of this cost. If the gap does not shrink as `--value_size` grows, the cost stays fixed per request and does not grow with byte count. Try this fix first: combine more writes into each Azure request, through a larger WAL flush buffer and fewer, larger page-blob writes.

2. **`rocksdb.manifest.file.sync.micros`.** This event is infrequent, but also network-bound. Optimize this path only when MANIFEST churn is high, for example under many small DB reopens or compactions.

3. **Read-path cache effectiveness**, through `rocksdb.block.cache.hit`/`rocksdb.block.cache.miss` from `--statistics=1`. A high miss rate under a workload larger than memory sends every miss to Azure over the network. Adjust the block cache size, or attach an AVEVA secondary cache with `--secondary_cache_uri` (see "Attaching a secondary cache" above) to reduce this cost. When comparing backends, prefer the CacheLib backend's `SECONDARY_CACHE_*` tickers over the FileBased backend's — see the known caveat above.

4. **Concurrent benchmarks, with `--threads>1`.** Real deployments run concurrent workloads. A `fillseq`/`readrandom`-only baseline does not show lock contention or lease-renewal thread overhead. Run the `db_bench` benchmarks named for concurrent read and write mixes, and profile the `BlobFilesystemImpl` lease-renewal thread under load.

5. **Compaction behavior on Azure.** SST files are also page blobs, and compaction rewrites many of these files. Watch `rocksdb.compaction.times.micros` and the total bytes written (`rocksdb.bytes.per.write` times op count). Check whether compaction raises Azure write volume more than expected.

## Cleanup

Delete a practice or test container when you finish. Use the
service-principal login from above, or the Azure Python SDK.

```powershell
az storage container delete --account-name $env:AZURE_STORAGE_ACCOUNT_NAME --name $env:AVEVA_DB_BENCH_CONTAINER --auth-mode login
```

## Notes

- `db_bench` command-line flags stay the same as upstream. See the
  [upstream docs](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools)
  and `db_bench.exe --help` for the full flag list.