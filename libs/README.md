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
| `aveva-http-client` | `aveva::http-client` | `boost_asio` | `boost_beast`, `boost_intrusive`, `boost_unordered`, `boost_url`, OpenSSL |
| `aveva-azure-client` | `aveva::azure-client` | `aveva-http-client`, `boost_asio` | OpenSSL, `boost_json`, `boost_url`, `boost_uuid`, LibXml2 (config package) |

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

The public headers of both libraries expose Boost.Asio types on purpose (`executor_type`, `get_executor()`,
`boost::asio::default_completion_token`, cancellation slots and associated executors). This is what lets callers
choose a completion token (`use_future`, `use_awaitable`, a plain callback, ...) and supply their own `io_context`.
Treat the Asio version as part of the API/ABI contract: consumers must build against the same Boost.Asio that these
libraries were built with, and changing it is a breaking change. The libraries never own or run an `io_context`; the
host injects one. Redesigning the API to hide Asio is deliberately out of scope.

### Completion contract

Every `...Async` operation (both libraries) completes its handler exactly once and never from inside the initiating
call; failures detected at initiation are delivered through the handler, not thrown. This does not mean the
completion cannot run on another I/O thread before the initiating thread returns, so synchronize shared state. The
handler runs on its associated executor (else the client's); a strand or a different `io_context` is honoured, and
the client holds work on both until delivery. If an executor keeps rejecting work, the handler runs once on the
completing thread rather than being lost. The HTTP client and its `io_context` must outlive every in-flight
operation (or be shared through the `std::shared_ptr` constructors); the blob client object itself may be moved from
or destroyed after initiating, since operations snapshot its state.

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
4. The plugin tests reuse `FakeHttpClient.hpp` and `TestFixtures.hpp` from `libs/AzureClient/tests` through the
   `aveva-azure-client-test-support` INTERFACE target (`infrastructure/cmake/AvevaClientLibraries.cmake`). Check
   that both headers still exist after a sync.
5. Rebuild and run `ctest -E Integration`.

## Building the library tests

The libraries' own test suites are built whenever the plugin tests are (`AVEVA_ROCKSDB_TESTS`, ON by default).
`-DAVEVA_ROCKSDB_TESTS=OFF` disables all of them, so GTest (the root vcpkg `testing` feature) is not needed.
CI (`.github/workflows/build-and-test.yml`) builds with tests on, and also
configures and builds a tests-off tree without the `testing` feature.
