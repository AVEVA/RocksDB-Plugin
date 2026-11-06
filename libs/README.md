# Vendored libraries

| Library | Path | Origin | Vendored version |
|---------|------|--------|------------------|
| aveva-http-client | `libs/HttpClient` | Maintained in-tree; no upstream repository is tracked | 0.0.1, imported in `af31859`; locally modified (see below) |
| aveva-azure-client | `libs/AzureClient` | Maintained in-tree; no upstream repository is tracked | 0.0.1, imported in `af31859`; locally modified (see below) |

Both libraries are licensed under Apache-2.0 (see the root `LICENSE` file); source files carry SPDX
identifiers inline.

The root `vcpkg.json` is the only manifest used by this repository's build, and the root `.clang-format`,
`.clang-tidy` and `.editorconfig` apply to these libraries.

## Installed packages and consuming them

Both libraries are installed as static-only CMake packages (`aveva_install_library`, config files under
`share/<package>/`). The package names, targets and the `find_package` calls a consumer needs:

| Package | Imported target | Public dependencies (resolved by its config file) | Link-only dependencies (private, still required at link time) |
|---------|-----------------|---------------------------------------------------|----------------------------------------------------------------|
| `aveva-http-client` | `aveva::http-client` | `boost_asio`, `Microsoft.GSL` | `boost_beast`, `boost_intrusive`, `boost_scope`, `boost_unordered`, `boost_url`, OpenSSL |
| `aveva-azure-client` | `aveva::azure-client` | `aveva-http-client`, `boost_asio`, `Microsoft.GSL` | OpenSSL, `boost_json`, `boost_url`, `boost_uuid`, LibXml2 (config package) |

A consumer only calls `find_package(aveva-azure-client CONFIG REQUIRED)` (and `aveva-http-client` if it uses the HTTP
client directly); the config files call `find_dependency` for everything in the table, so Boost, OpenSSL and LibXml2
must be discoverable (for example through the same vcpkg toolchain and manifest). The plugin packages
`aveva-rocksdb-plugin-core` and `aveva-rocksdb-plugin-azure` (and `-azure-impl`) in turn depend on both libraries.
The `.github/workflows/build-and-test.yml` `install-consume` job builds an external consumer against an install tree to
keep this list honest.

```cmake
find_package(aveva-azure-client CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE aveva::azure-client)
```

The `_WIN32_WINNT=0x0A00` definition is public on Windows so that consumers and both libraries agree on the Asio API
level.

## Boost.Asio is part of the public API

The public headers of both libraries expose Boost.Asio types on purpose (`awaitable`, `executor_type`,
`get_executor()` and cancellation slots). Every operation is a C++20 coroutine (`boost::asio::awaitable`); there are no
completion tokens, callbacks or futures, and callers supply their own `io_context`.
Treat the Asio version as part of the API/ABI contract: consumers must build against the same Boost.Asio that these
libraries were built with, and changing it is a breaking change. The libraries never own or run an `io_context`; the
host injects one. Redesigning the API to hide Asio is deliberately out of scope.

### Completion contract

Every `...Async` operation (both libraries) returns a lazy `boost::asio::awaitable`: nothing is sent until it is
`co_await`ed, and it completes exactly once. `HttpClient::SendAsync` yields `HttpResult`
(`std::expected<HttpResponse, std::error_code>`); an HTTP error status is a response, not an error. Failures are
reported in the result rather than thrown: a rejected request yields `HttpClientError::InvalidRequest` (or
`InvalidUrl`), `std::bad_alloc` yields `std::errc::not_enough_memory`, and any other exception
`std::errc::state_not_recoverable`. The AzureClient operations likewise yield
`std::expected<Response<T>, BlobStorageError>`.

- **Cancellation.** Each request is one coroutine, so cancelling the awaiting coroutine (through its cancellation
  slot) cancels the request, which then completes with `std::errc::operation_canceled`.
- **Timeout.** The per-request timeout (`HttpRequestOptions`) is a single deadline covering the whole request,
  including the wait for a per-origin connection slot. When it fires the result is `HttpClientError::TimedOut` and a
  `Timeout` diagnostic (with the request phase) is reported. Very large timeouts are clamped, not overflowed.
- **Executor affinity.** The request runs on a strand of the awaiting coroutine's executor (the strand serializes the
  deadline timer with the request's completion), and the awaiting coroutine resumes on its own executor. Await from
  a coroutine running on the `io_context` the client was created with (`get_executor()`), and never block on the
  result from a thread that runs that `io_context`.
- **Lifetime.** The caller owns the `io_context` and must keep it alive and running while requests are outstanding.
  The `HttpClient` object may be destroyed while a request is running (the request holds the state it needs);
  destroying it fails requests still waiting for a per-origin slot.

## Local modifications

Both libraries have diverged from the imported 0.0.1 tree. The changes are in the commit range
`af31859..HEAD -- libs` (`git log af31859..HEAD -- libs`) and fall into two groups:

- Security, correctness, performance, test and documentation fixes from review backlogs (`8be03b2`, `0b6df28`,
  `94d4814`), for example stricter URL and identity-endpoint validation and response parsing.
- Trimming of `libs/AzureClient` to what the plugin needs (`2cf0763`..`da22526`): AppendBlobClient, uncommon
  BlobClient/BlockBlobClient operations, container/service extras and snapshot/version scoping were removed.
  SAS builders (`Sas.hpp`), `BlobServiceClient::GetUserDelegationKeyAsync` and the internal `Redaction.hpp`
  helper were later removed as well, since the plugin never used them. `PageBlobClient::GetPageRangesAsync` is kept:
  the plugin uses it (`BlobOperations.hpp`).

## Also removed

`BlockBlobClient` (header, sources, plugin typedef and instantiation), `PageBlobClient::ClearPagesAsync`,
`BlobContainerClient::CreateAsync`/`GetBlobClient`/`GetBlockBlobClient`, the stream overloads of
`DownloadToAsync` and the static bearer-token path (`StaticTokenCredential`, `RequestAuth::Kind::Bearer`)
were removed. A test-only `Tests::StaticTokenCredential` remains in `libs/AzureClient/tests/FakeHttpClient.hpp`.

## Updating

Do not copy an upstream tree over these directories: that would silently revert the local changes above.

1. Diff the new upstream release against the imported baseline (`af31859`) to get the upstream delta, and apply
   that delta (`git apply --3way` or a rebase of the local commits) onto the current tree. Resolve conflicts in
   favour of the local security fixes and keep the trimmed API surface.
2. Leave out standalone-only files (CI pipelines, `CMakePresets.json`, `vcpkg-configuration.json`,
   backlog files).
3. Update the version column above and add any new local commits to the range described in "Local modifications".
4. The plugin tests reuse `FakeHttpClient.hpp` and `TestSupport.hpp` from `libs/AzureClient/tests` through the
   `aveva-azure-client-test-support` INTERFACE target (`infrastructure/cmake/AvevaClientLibraries.cmake`). Check
   that both headers still exist after a sync.
5. Rebuild and run `ctest -LE integration`.

## Building the library tests

The libraries' own test suites are built whenever the plugin tests are (`AVEVA_ROCKSDB_TESTS`, ON by default).
`-DAVEVA_ROCKSDB_TESTS=OFF` disables all of them, so GTest (the root vcpkg `testing` feature) is not needed.
CI (`.github/workflows/build-and-test.yml`) builds with tests on, and also
configures and builds a tests-off tree without the `testing` feature.
