# Vendored libraries

| Library | Path | Origin | Vendored version |
|---------|------|--------|------------------|
| aveva-http-client | `libs/HttpClient` | Maintained in-tree; no upstream repository is tracked | 0.0.1, imported in `af31859`; locally modified (see below) |
| aveva-azure-client | `libs/AzureClient` | Maintained in-tree; no upstream repository is tracked | 0.0.1, imported in `af31859`; locally modified (see below) |

Both libraries are licensed under Apache-2.0 (see the root `LICENSE` file); source files carry SPDX
identifiers inline.

The root `vcpkg.json` is the only manifest used by this repository's build, and the root `.clang-format`,
`.clang-tidy` and `.editorconfig` apply to these libraries.

## Boost.Asio is part of the public API

The public headers of both libraries expose Boost.Asio types on purpose (`executor_type`, `get_executor()`,
`boost::asio::default_completion_token`, cancellation slots and associated executors). This is what lets callers
choose a completion token (`use_future`, `use_awaitable`, a plain callback, ...) and supply their own `io_context`.
Treat the Asio version as part of the API/ABI contract: consumers must build against the same Boost.Asio that these
libraries were built with, and changing it is a breaking change. The libraries never own or run an `io_context`; the
host injects one. Redesigning the API to hide Asio is deliberately out of scope.

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
