# Changelog

## 0.1.0

### Breaking changes
- `Plugin::Register` now takes a required `boost::asio::io_context&` (after the `guard` argument). The application owns the context, must keep it alive and running while the filesystem is in use, and must not run it on threads that call into RocksDB. Migration: create an `io_context`, run it on dedicated threads, and pass it to `Register`.
- The Azure SDK for C++ was replaced by the vendored `libs/AzureClient` and `libs/HttpClient` (Apache-2.0, see `libs/README.md`). Error mapping to `rocksdb::Status` was reworked: 401 and 403 are non-retryable IOErrors, 408/429, every 5xx status and transport failures are retryable IOErrors, and cancellation maps to `Aborted`. Unexpected exceptions map to a non-retryable `state_not_recoverable` error.
- The project now requires C++23 (vendored libraries included).
- `BindToRuntime` and the credential helpers (`CreateCredentialSources`, `CreateServiceClient`, ...) take `const std::shared_ptr<ClientRuntime>&`. `CachingTokenCredential` must be created with `CachingTokenCredential::Create(...)` (the constructor is no longer usable directly).
- Blocking on an Azure future from a thread running the injected `io_context` now throws `std::logic_error` instead of risking a deadlock.
- The public `Core::BlobClient` interface no longer uses `Azure::ETag` (`<azure/core/etag.hpp>` is no longer included). `GetEtag()` returns `std::string` and `Download(..., ifMatch)` takes `const std::string&`; a `BlobMetadata` struct and the virtuals `GetMetadata`, `DownloadAsync` (two overloads) and `GetMetadataAsync` were added (all with default implementations). Migration: change ETag parameters and return types to `std::string` in your implementations and mocks; the new virtuals need no override, but override them for truly asynchronous I/O.

### Behavior changes
- Async IO is now advertised (`SupportedOps` returns `1 << kAsyncIO`; previously async IO was effectively off) and `ReadAsync`/`Poll`/`AbortIO` are implemented.
- Credential chain: a managed identity is always tried (system-assigned when no id is given); environment and workload identity credentials are only added when their variables are set. See the README.
- `DeleteDir` throws when any blob deletion fails and counts remaining blobs across all pages.
- Reads of a blob that keeps changing now fail with an IOError after 5 refreshes instead of looping forever.
- Lease IDs are random UUIDs generated once per acquisition; lease renewal no longer blocks `LockFile`/`UnlockFile`.
- Directory listings read file sizes from blob metadata in the listing instead of one request per blob.
- `aveva-rocksdb-plugin-azure-impl` exports `_WIN32_WINNT=0x0A00` as a PUBLIC compile definition on Windows (Boost.Asio is part of its public headers). Consumers must not target an older Windows version; `ClientRuntime.hpp` fails with `#error` if `_WIN32_WINNT` is lower than `0x0A00`.
- The TLS trust store can be overridden with the standard `SSL_CERT_FILE` / `SSL_CERT_DIR` environment variables; when set, they are used instead of the exported Windows root certificates.
- Shared Key signing now signs the percent-encoded request path as sent and orders `x-ms-*` headers with the service's culture-aware comparison. Status-0 transport failures that cannot succeed on retry (invalid arguments, authentication failures, malformed responses) map to non-retryable errors.

### Dependency updates
- vcpkg `builtin-baseline` `9e593bb` -> `c748cb4`: rocksdb 11.1.2 -> 11.8.1, boost 1.91.0 -> 1.92.0, openssl 3.6.3 -> 3.6.5, gtest 1.17.0 -> 1.18.0, libxml2 2.15.3 -> 2.15.4 (zstd unchanged at 1.5.7). Dropped `azure-identity-cpp` and `azure-storage-blobs-cpp`.
- RocksDB 11.2-11.8.1 release notes reviewed: the only `FileSystem` changes are additive (`FileSystem::SyncFile`, `FSRandomAccessFile::SubmitReadAsync`/`GetReadExecutor`, `DBOptions::read_io_executor_threads`) and the plugin compiles unchanged. `CompressedSecondaryCacheOptions::compress_format_version` was removed in 11.0.0 (inside the previous baseline's range) and is not used here. Unit (191) and integration (93) tests pass against a real storage account.
