<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright 2025 AVEVA -->

# Changelog

## 0.1.0

### Breaking changes
- `Plugin::Register` now takes a required `boost::asio::io_context&` (after the `guard` argument). The application owns the context, must keep it alive and running while the filesystem is in use, and must not run it on threads that call into RocksDB. Migration: create an `io_context`, run it on dedicated threads, and pass it to `Register`.
- The Azure SDK for C++ was replaced by the vendored `libs/AzureClient` and `libs/HttpClient` (Apache-2.0, see `libs/README.md`). Error mapping to `rocksdb::Status` was reworked: 401 and 403 are non-retryable IOErrors, 408 and transport timeouts are retryable `TimedOut`, 429 and 503 are retryable `Busy`, other 5xx statuses and transport failures are retryable IOErrors, and cancellation maps to `Aborted`. Unexpected exceptions map to a non-retryable `state_not_recoverable` error.
- The project now requires C++23 (vendored libraries included).
- `BindToRuntime` and the credential helpers (`CreateCredentialSources`, `CreateServiceClient`, ...) take `const std::shared_ptr<ClientRuntime>&`. `CachingTokenCredential` must be created with `CachingTokenCredential::Create(...)` (the constructor is no longer usable directly).
- Blocking on an Azure future from a thread running the injected `io_context` now throws `std::logic_error` instead of risking a deadlock.
- The public `Core::BlobClient` interface no longer uses `Azure::ETag` (`<azure/core/etag.hpp>` is no longer included). `GetEtag()` returns `std::string` and `Download(..., ifMatch)` takes `const std::string&`; a `BlobMetadata` struct and the virtuals `GetMetadata`, `DownloadAsync` (two overloads), `GetMetadataAsync` and `UploadPagesAsync` were added (all with default implementations). Migration: change ETag parameters and return types to `std::string` in your implementations and mocks; the new virtuals need no override, but override them for truly asynchronous I/O.

- Other public API/ABI breaks: `AzureErrorTranslator::IOStatusFromError` takes `unsigned int` instead of `Azure::Core::Http::HttpStatusCode`; `BlobFilesystem::m_lockFiles` changed type and a mutex was added; `BlobFilesystem::LogRequestFailed` takes the plugin's `RequestFailedException`; `ReadableFile` holds a `shared_ptr` and adds `ReadAsync`; the new `Core::BlobClient` virtuals change the vtable layout, so custom implementations and mocks must be rebuilt. No `Azure::Core` / `Azure::Storage` types remain in the public API.
- The plugin registry name changed from `"azblobfs" + dbName` to `"azblobfs-" + <hex of account URL and db name>`; anything that looked up the Env/FileSystem by the old name (options files, URIs) must use the new name.

### Behavior changes
- `FileExists` on a directory now matches only blobs under `<path>/`, so `foo` no longer matches a sibling `foobar`.
- Acquire Lease always sends a lease ID (a generated one when `ProposedLeaseId` is empty) so retries stay idempotent. `AuthorityHost` now rejects userinfo (`@`) and backslashes. HttpClient clamps very large timeouts to one year.
- Async IO is now advertised (`SupportedOps` returns `1 << kAsyncIO`; previously async IO was effectively off) and `ReadAsync`/`Poll`/`AbortIO` are implemented.
- Credential chain: a managed identity is always tried (system-assigned when no id is given); environment and workload identity credentials are only added when their variables are set. See the README.
- `DeleteDir` throws when any blob deletion fails and counts remaining blobs across all pages.
- Reads of a blob that keeps changing now fail with an IOError after 5 refreshes instead of looping forever.
- Lease IDs are random UUIDs generated once per acquisition; lease renewal no longer blocks `LockFile`/`UnlockFile`.
- Directory listings read file sizes from blob metadata in the listing instead of one request per blob.
- `aveva-rocksdb-plugin-azure-impl` exports `_WIN32_WINNT=0x0A00` as a PUBLIC compile definition on Windows (Boost.Asio is part of its public headers). Consumers must not target an older Windows version; `ClientRuntime.hpp` fails with `#error` if `_WIN32_WINNT` is lower than `0x0A00`.
- The TLS trust store can be overridden with the standard `SSL_CERT_FILE` / `SSL_CERT_DIR` environment variables; when set, they are used instead of the exported Windows root certificates.
- Shared Key signing now signs the percent-encoded request path as sent and orders `x-ms-*` headers with the service's culture-aware comparison. Status-0 transport failures that cannot succeed on retry (invalid arguments, authentication failures, malformed responses) map to non-retryable errors.
- `ReadableFile` overrides `MultiRead` (the downloads of a batch overlap) and `Prefetch` (a per-file buffer of up to 2 ranges, capped at 256 MiB across files; `Prefetch` returns `NotSupported` when both ranges are still downloading or the cap is reached, so RocksDB uses its own readahead). `ReadAsync` never waits for a pending prefetch.
- `WriteableFile` overrides `RangeSync`, which starts page uploads without waiting for them (`strict_bytes_per_sync` defers to `Sync`). Page uploads are pipelined with up to 4 in flight per file; asynchronous uploads send whole pages only, and the trailing partial page is uploaded by `Flush`/`Sync`/`Close`. The first upload failure is sticky for later `Sync`/`Close`, and destroying a file waits for its outstanding uploads. The data file buffer size is rounded down to a whole number of pages.
- Access tokens that live longer than 24 hours are accepted and refreshed at the 24 hour mark instead of being rejected.
- HttpClient: `HttpRequestOptions::SetResponseHeaderLimit` (default 64 KiB, previously Beast's fixed 8 KiB); an EOF-delimited HTTPS response is accepted when the server closes TCP without a TLS `close_notify`.
- New `Plugin::NameFor` returns the registry name `Register` uses.
- New `tools/db_bench` (`aveva_db_bench`, `--fs_uri=auto`) and `tools/db_bench/Compare-Backends.ps1` for comparing backends.

### Dependency updates
- vcpkg `builtin-baseline` `9e593bb` -> `c748cb4`: rocksdb 11.1.2 -> 11.8.1, boost 1.91.0 -> 1.92.0, openssl 3.6.3 -> 3.6.5, gtest 1.17.0 -> 1.18.0, libxml2 2.15.3 -> 2.15.4 (zstd unchanged at 1.5.7). Dropped `azure-identity-cpp` and `azure-storage-blobs-cpp`.
- RocksDB 11.2-11.8.1 release notes reviewed: the only `FileSystem` changes are additive (`FileSystem::SyncFile`, `FSRandomAccessFile::SubmitReadAsync`/`GetReadExecutor`, `DBOptions::read_io_executor_threads`) and the plugin compiles unchanged. `CompressedSecondaryCacheOptions::compress_format_version` was removed in 11.0.0 (inside the previous baseline's range) and is not used here. Unit and integration tests pass against a real storage account.
