# `aveva_db_bench` — RocksDB `db_bench` with the AVEVA Azure plugin

`aveva_db_bench` is a thin wrapper around Facebook's
[`db_bench`](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools) tool.
The wrapper registers the AVEVA Azure Page Blob filesystem plugin with the
RocksDB `ObjectLibrary`. The wrapper does this **before** it calls
`rocksdb::db_bench_tool`. This lets `db_bench` send I/O through Azure Blob
Storage, the same way a production consumer of the plugin does.

This tool is a POC deliverable for work item **4983896**.

## Build

The wrapper is off by default. To build it, use any preset and pass the CMake option:

```powershell
cmake --preset WindowsRelease -DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON
cmake --build build/WindowsRelease --config Release --target aveva_db_bench
```

Setting `-DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON` also turns on the `db-bench` vcpkg
manifest feature, which builds RocksDB's `db_bench_tool` through the overlay port.
Use a fresh build directory, or `--fresh`, when you toggle the option so that vcpkg
reinstalls RocksDB. See the root
[README.md](../../README.md#building) for preset details and how to configure manually.

This build uses an [overlay port](../../infrastructure/vcpkg-overlays/rocksdb).
The overlay keeps `WITH_BENCHMARK_TOOLS=OFF` and instead applies
`0004-install-db-bench-tool-lib.patch`, which exports `RocksDB::db_bench_tool` as
an installable static library that the wrapper links against.

## Run — local filesystem (smoke test)

The wrapper always registers the Azure plugin, so it requires every `AVEVA_DB_BENCH_*` variable
listed below and exits with status 2 when one is missing; there is no pass-through mode. Without
`--fs_uri` the benchmark still runs on the local filesystem, which makes it a quick check of the build and a
baseline to compare against.

```powershell
./build/WindowsRelease/tools/db_bench/Release/db_bench.exe `
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

- The plugin creates the container if it does not exist, so a single-run benchmark needs no setup.
  Compare-Backends.ps1` is the exception: it expects its two containers to exist already (see below).

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
$exe = ".\build\WindowsRelease\tools\db_bench\Release\db_bench.exe"

& $exe `
    --benchmarks=fillseq,readrandom `
    --num=10000 `
    --compression_type=zlib `
    --fs_uri=auto `
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
    --fs_uri=<plugin name>   (or pass --fs_uri=auto and the wrapper substitutes it)
    --db=<account>+<container>/<your-db-name>
```

If the tool instead exits with status 2 and prints `missing required environment variable`, you did not set
all of the env vars above in that shell before you ran the tool.

Do not read the console output by hand to check this line. Capture stderr
and search it instead.

```powershell
$out = & $exe --benchmarks=fillseq --num=10 --compression_type=zlib `
    --fs_uri=auto `
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
    --fs_uri=auto `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db"
```
Check the `readrandom` result line for `(10000 of 10000 found)`.

## Stats and histograms

Add these flags to get latency percentiles and RocksDB's internal counters.

```powershell
& $exe --benchmarks=fillrandom,readrandom --num=50000 --value_size=1024 `
    --compression_type=zlib --statistics=1 --histogram=1 `
    --fs_uri=auto `
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
    --fs_uri=auto `
    --db="$($env:AZURE_STORAGE_ACCOUNT_NAME)+$($env:AVEVA_DB_BENCH_CONTAINER)/bench-db"
```

Compare the two output files side by side.

Check these signals, in priority order, for follow-up investigation:

1. **`rocksdb.db.write.micros` P50/P99, against the local baseline.** In illustrative manual runs (machine, Azure region and commit were not recorded, so treat the numbers as indicative only), the plugin's per-write overhead was near 40-50 ms median, against sub-millisecond on local disk. Record your own machine, region and commit when you compare results. Network round-trips to Azure cause most of this cost. If the gap does not shrink as `--value_size` grows, the cost stays fixed per request and does not grow with byte count. Try this fix first: combine more writes into each Azure request, through a larger WAL flush buffer and fewer, larger page-blob writes.

2. **`rocksdb.manifest.file.sync.micros`.** This event is infrequent, but also network-bound. Optimize this path only when MANIFEST churn is high, for example under many small DB reopens or compactions.

3. **Read-path cache effectiveness**, through `rocksdb.block.cache.hit`/`rocksdb.block.cache.miss` from `--statistics=1`. A high miss rate under a workload larger than memory sends every miss to Azure over the network. Adjust the block cache size, or use the plugin's secondary cache (`Core/FileBasedCompressedSecondaryCache`), to reduce this cost.

4. **Concurrent benchmarks, with `--threads>1`.** Real deployments run concurrent workloads. A `fillseq`/`readrandom`-only baseline does not show lock contention or lease-renewal thread overhead. Run the `db_bench` benchmarks named for concurrent read and write mixes, and profile the `BlobFilesystemImpl` lease-renewal thread under load.

5. **Compaction behavior on Azure.** SST files are also page blobs, and compaction rewrites many of these files. Watch `rocksdb.compaction.times.micros` and the total bytes written (`rocksdb.bytes.per.write` times op count). Check whether compaction raises Azure write volume more than expected.

## Cleanup

Delete a practice or test container when you finish. Use the
service-principal login from above, or the Azure Python SDK.

```powershell
az storage container delete --account-name $env:AZURE_STORAGE_ACCOUNT_NAME --name $env:AVEVA_DB_BENCH_CONTAINER --auth-mode login
```

## Comparing two builds — `Compare-Backends.ps1`

`Compare-Backends.ps1` (PowerShell 7.2 or later) runs the same db_bench scenarios against real Azure Blob Storage with
two executables and prints a side-by-side table: `-BaselineExe` (for example built from `main` with the tool
backported) and `-CandidateExe` (for example this branch). `-BaselineFsUri` and `-CandidateFsUri` set the `--fs_uri`
each one receives; `auto` means the plugin name the wrapper registered.

- **Build the candidate executable** with a preset plus the option, which also activates the overlay port through
  `VCPKG_OVERLAY_PORTS` (the root `CMakeLists.txt` appends `infrastructure/vcpkg-overlays/rocksdb` for you):

  ```powershell
  cmake --preset WindowsRelease -DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON
  cmake --build build/WindowsRelease --config Release --target aveva_db_bench
  ```

  If you configure without the option, `tools/db_bench` stops with an error because `RocksDB::db_bench_tool` is
  missing; use a fresh build directory when you toggle it.
- **Credentials** come from `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_TENANT_ID`, `AZURE_SERVICE_PRINCIPAL_ID` and
  `AZURE_SERVICE_PRINCIPAL_SECRET`. The service principal needs **Storage Blob Data Contributor** on the storage
  account, or on both containers `<ContainerPrefix>-baseline` and `<ContainerPrefix>-candidate` (default prefix
  `bench`), which must
  already exist.
- **Outputs** go to `-OutDir` (default `db_bench_compare_<timestamp>` under your temp directory): one log per db_bench
  run, `summary.csv`, `tail-latency.csv` (p50/p99 from the db_bench histograms) and `run-metadata.json` (repository
  revision and dirty flag, executable hashes, configuration, build tree compiler/triplet, vcpkg baseline, host, scenario
  parameters, the exact arguments of every run; never credentials). The script never deletes them, and it scrubs the
  client secret from every log. Delete the folder, and the test blobs left in the two containers, when you finish.
- **Live runs are opt-in.** The script spends cloud resources, so it refuses to start without `-ConfirmLiveAzure`.
  Use Release builds; a warning is printed for any other configuration. Pass `-BaselineRevision` /
  `-CandidateRevision` when the executables were not built from this checkout's HEAD.

```powershell
.\tools\db_bench\Compare-Backends.ps1 -BaselineExe <main db_bench.exe> -CandidateExe .\build\WindowsRelease\tools\db_bench\Release\db_bench.exe -Quick -ConfirmLiveAzure
```

### Loopback CPU and allocation measurements — `Measure-Loopback.ps1`

These need no Azure account and are **not** Azure network results. Three benchmarks cover three different scopes; do not
compare numbers across them, and none of them is a live Azure measurement:

| Scope | Executable | Path exercised | Not included |
|-------|------------|----------------|--------------|
| HTTP only | `aveva-http-client-tests` | `HttpClient::SendAsync` over a warm pooled loopback TCP connection | Azure client, plugin, TLS |
| Azure client | `aveva-azure-client-e2e-benchmark` | `BlobClient` request pipeline, real `HttpClient`, loopback TCP to a page-blob server on its own thread, into caller-owned spans | plugin (`AsyncReadRequest`, download pool, prefetch, MultiRead merge/split, write pipeline), TLS (plain HTTP, fake SAS) |
| Plugin | `aveva-rocksdb-plugin-azure-impl-benchmark` | the plugin's readable/writeable file classes over the real `BlobClient` and an in-process `FakeBlobServer` through the fake HTTP transport | sockets, TLS, `HttpClient`, network latency |
| Live Azure | `Compare-Backends.ps1 -ConfirmLiveAzure` | `db_bench` against a real account | explicit opt-in only |

What the allocation counters mean: every benchmark replaces scalar `operator new` only. They do not see aligned new,
direct `malloc`, OpenSSL/TLS allocations (OpenSSL uses its own allocator), the OS, or peak process memory. The HTTP and
Azure-client benchmarks count the client thread only (thread-local, so the server and any worker thread are excluded).
The plugin benchmark reports `thread_*` (the threads issuing the operations, summed) and `process_*` (all threads,
including the fake transport's pump and plugin workers); because the fake service runs inline, its response-body
copies are inside both, so use plugin numbers for before/after comparison on one build rather than as absolute plugin
cost. `peak_live_bytes` and `retained_bytes` are growth of the process-wide live heap (allocator usable sizes) relative
to the start of the measured window: peak while it ran, and what was still held when it ended (pooled buffers, caches,
prefetch results); a negative value means memory that existed before the window, such as consumed prefetch slots, was
released. Timings (`p50_us`/`p99_us`, `mib_per_second`) are reported, never asserted.

- `aveva-http-client-tests --gtest_filter=HttpClientAllocations.PublicClientPooledRequestWithIndependentServer`:
  the public `HttpClient::SendAsync` (including its final completion post) over a warm pooled connection, 0 B and
  16 KiB bodies. The older `SubmitAllocationsPerRequest` test stays as a coarse regression check of the private core.
- `aveva-azure-client-e2e-benchmark`: `BlobClient::DownloadToAsync` ranged reads through the request pipeline, the real
  `HttpClient` and TCP to a loopback page-blob server (4 KiB and 64 KiB reads, 1 and 8 reads in flight).
- `aveva-rocksdb-plugin-azure-impl-benchmark`: 4 MiB random reads, 16 merged 4 KiB `MultiRead` requests, 4 MiB
  prefetch-hit reads and 4 KiB append+`Sync` writes, at 1 and 4 threads where that is meaningful. It reports requests
  the service saw (`downloads`, `uploads`, `properties`), allocations and bytes per operation, peak/retained live heap,
  throughput and p50/p99 latency. Its tests assert only request counts and correctness, not timings.

```powershell
cmake --preset WindowsRelease
cmake --build build\WindowsRelease --config Release --target aveva-http-client-tests aveva-azure-client-e2e-benchmark aveva-rocksdb-plugin-azure-impl-benchmark
.\tools\db_bench\Measure-Loopback.ps1 -Label before   # then rebuild at the other revision and run with -Label after
```

The script finds the binaries for either layout: multi-config trees (`...\Release\x.exe`, Visual Studio) and
single-config trees (`...\x.exe`, Ninja, as `WindowsRelease` is). If several configurations of one target exist it
asks for `-Configuration`; `-HttpClientExe`, `-AzureClientExe` and `-PluginExe` take explicit paths. The recorded
configuration comes from the path or else the build tree's `CMAKE_BUILD_TYPE` (so `RelWithDebInfo` is reported as such).
The metadata records the repository HEAD at run time but the executables' own revision as `unknown`: HEAD is not proof
the binary was built from it, so rebuild before measuring and check `lastWriteUtc`. Only Windows has been exercised;
the helper looks up suffix-less names elsewhere, but that is untested and not cross-platform evidence.
Script tests: `Invoke-Pester tools\db_bench\BenchmarkMetadata.Tests.ps1`.

Each `BENCH` line reports its counters per request or operation; `loopback-metadata.json` records the revision,
hashes, build tree and host. Compare allocation counts across revisions (they are stable); treat timings as noisy
and use `-Repeat`.

## Notes

- `db_bench` command-line flags stay the same as upstream. See the
  [upstream docs](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools)
  and `db_bench.exe --help` for the full flag list.