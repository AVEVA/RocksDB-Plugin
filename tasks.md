# Review Tasks — `feature/replace-azure-sdk`

Review of `feature/replace-azure-sdk` vs `origin/main` (merge-base `278868c`).
Commits reviewed: `af31859` (replace Azure SDK with vendored `libs/AzureClient` + `libs/HttpClient`),
`7d4dac2` (inject `io_context` via `Plugin::Register`), `6a192e4` (async FS reads, RocksDB 11.8.1 via vcpkg baseline bump).

Scope: plugin code (`src/`, `include/`, `tests/`, CMake, vcpkg, docs) plus how `libs/` is integrated.
The ~40k lines of vendored library code were **not** reviewed line by line.

## Validation baseline (at review time)

| Command | Result |
|---|---|
| `cmake --build build\WindowsDebug --config Debug` | Passed (exit 0) |
| `ctest --test-dir build\WindowsDebug -C Debug -E "Integration" -j 8` | Passed — 154/154 |
| Integration tests (need `AZURE_*` env vars) | Not run |

## Executor rules

- Work task by task in priority order. Keep each change small and surgical.
- After each native change: `cmake --build build\WindowsDebug --config Debug`, then
  `ctest --test-dir build\WindowsDebug --output-on-failure --build-config Debug -E Integration`.
- New test files must be added to `tests/AVEVA/RocksDB/Plugin/Azure/Impl/CMakeLists.txt` (or the owning test CMakeLists).
- Run `clang-format` on touched files under `src/`, `include/`, `tests/`.
- Map Azure client errors (`AVEVA::Azure::RequestFailedException`) to meaningful `rocksdb::Status`; never swallow errors.
- **Never create or publish a PR without explicit user approval.**
- Tick the checkbox when a task is done; add a one-line note if you deviated.

---

## P0 — Merge blockers

### [x] T01 (deviation: REUSE.toml covers libs/** instead of per-file SPDX headers) — Resolve licensing of vendored libraries
- **Files:** `libs/AzureClient/LICENSE`, `libs/HttpClient/` (no LICENSE), root `LICENSE` (Apache-2.0), `THIRD_PARTY_NOTICES*`
- **Problem:** `libs/AzureClient/LICENSE` is proprietary ("internal use only… not … published, distributed … outside AVEVA Group"). This repo is Apache-2.0 and hosted at `AVEVA/RocksDB-Plugin`, possibly public. `libs/HttpClient` has no license file, and none of the sources in either library have SPDX headers.
- **Required:** **Needs human decision; do not decide this yourself.** Options: (a) relicense both libs to Apache-2.0, add `LICENSE` and SPDX headers; (b) stop vendoring and consume them as a private vcpkg port or package. Record the decision here, then update the third-party notices to match.
- **Done when:** every file under `libs/` has a license compatible with the repo and its distribution model.

### [x] T02 — Clean up vendoring artifacts and record provenance
- **Files:** `libs/AzureClient/{tasks.md, azure-pipelines.yml, pipelines/, CMakePresets.json, .git-blame-ignore-revs, vcpkg-configuration.json, CHANGELOG.md}`, `libs/HttpClient/{CMakePresets.json, vcpkg.json}`
- **Problem:** The libraries were copied in along with standalone-repo files that are unused here. `libs/AzureClient/tasks.md` is a stale backlog. In `libs/HttpClient/vcpkg.json`, `gtest` is a non-optional dependency.
- **Required:** Remove the CI, preset, and backlog files that the root build does not use. Make `gtest` a test-only feature, or confirm the root `vcpkg.json` is the only manifest used. Add `libs/README.md` that records the upstream repo URL and the commit or version vendored for each library, plus how to update them.
- **Verify:** fresh configure plus build (`cmake --preset <preset> --fresh`) still succeeds.

---

## P1 — Correctness

### [x] T03 — Fix and extend `AzureErrorTranslator`
- **File:** `src/Azure/AzureErrorTranslator.cpp`
- **Problems:**
  1. In the `RequestTimeout` case, `SetRetryable` is called on a temporary that is then discarded, and a new, non-retryable `TimedOut` is returned. The bug predates this branch, but this code was touched here.
  2. Status code `0` (transport, TLS, DNS, or credential failures, now carrying `RequestFailedException::Code`) is not differentiated.
  3. Missing mappings: 403, 409, 412, 429, 500, 502, and 504.
- **Required:**
  - Return a retryable `TimedOut` for 408 and for transport timeout or cancellation.
  - Return a retryable `IOError` for connection failures.
  - 403 → non-retryable `IOError` with a clear "authorization failed" message.
  - 409 and 412 → non-retryable `IOError` that preserves the error code.
  - 429 and 503 → retryable `Busy`.
  - 500, 502, 504 → retryable `IOError`.
  - Always include the Azure error code and message in the status text.
- **Tests:** add `AzureErrorTranslatorTests.cpp` covering every branch, including the retryable flag.

### [x] T04 (deviation: 409 still retried as normal lease contention) — Make lease acquisition idempotent in `LockFileImpl::Lock`
- **File:** `src/Azure/Impl/LockFileImpl.cpp`
- **Problems:**
  1. Each retry generates a new random `ProposedLeaseId`. If an acquire succeeds on the server but the response is lost, later retries return 409 until the lease expires.
  2. `NewLeaseId` uses `std::mt19937_64` instead of a real UUID generator.
  3. The retry loop also retries non-transient errors such as 403.
- **Required:**
  - Generate the proposed lease ID once, before the loop, and reuse it on every retry.
  - Use `boost::uuids::random_generator` (boost-uuid is already a dependency).
  - Fail fast on errors that are not transient (anything other than 408, 429, 5xx, or a transport error). Reuse T03's classification.
- **Tests:** mock-based test where the first acquire throws a transient error and the second succeeds; assert the same lease ID was sent both times. Add a test that 403 fails without retrying.

### [x] T05 — Fetch blob size and ETag in a single request
- **Files:** `include/AVEVA/RocksDB/Plugin/Core/BlobClient.hpp`, `src/Azure/Impl/PageBlob.{hpp,cpp}`, `src/Azure/Impl/ReadableFileImpl.cpp` (constructor and `RefreshBlobMetadata`), `tests/**/BlobClientMock*`
- **Problem:** `GetSize()` and `GetEtag()` each send their own GetProperties request. That is two round trips per open or refresh, and the two values can come from different versions of the blob (a race).
- **Required:** add a synchronous `GetMetadata()` that returns `{size, etag}` from one GetProperties call, mirroring the existing `GetMetadataAsync`. Use it in `ReadableFileImpl` and update the mock. Keep the old methods if other code calls them.
- **Tests:** update the ReadableFileImpl tests to expect one metadata call per open or refresh.

### [x] T06 — Fix `PageBlob::DownloadTo` edge cases
- **File:** `src/Azure/Impl/PageBlob.cpp`
- **Problems:**
  1. `DownloadTo(span)` copies `min(content.size(), buffer.size())` bytes but returns `ContentRange->Length`, which can be larger than the number of bytes copied.
  2. `DownloadTo(path, offset, length)` with `length == 0` sends an explicit zero-length range, which AzureClient rejects. FileCache calls it with `fileSize == 0` for empty files.
- **Required:** (1) return the number of bytes actually copied; (2) when `length == 0`, create or truncate an empty local file and return without making a request.
- **Tests:** unit or integration coverage for both cases; FileCache test with an empty blob.

### [x] T07 (RuntimeBoundCredential decorator; dangling io_context reference is documented, not detectable) — Confirm lifetimes of `io_context` and `IHttpClient`
- **Files:** `src/Azure/Plugin.cpp`, `src/Azure/Impl/ClientRuntime.cpp`, `src/Azure/Impl/TokenCredentials.cpp`
- **Problems:**
  1. The filesystem factory captures `boost::asio::io_context&`. If the host destroys the context without re-registering, the reference dangles. This is documented, but nothing enforces it.
  2. Credential retry timers in `SendWithRetry` capture `IHttpClient&` by reference only. An in-flight token refresh, for example during an aborted async read, may outlive `ClientRuntime`.
- **Required:**
  - Make the credential and retry continuations hold a `shared_ptr` to the runtime or HTTP client, or otherwise prove they cannot outlive it. Add a comment explaining why the result is safe.
  - For (1), document the contract prominently in `Plugin.hpp`. Add a debug assertion or log line if detection is feasible.
- **Tests:** a test that destroys the filesystem while a token refresh is pending, if a fake credential makes that possible.

### [x] T08 (deviation: aggregated error is logged per source, not carried in the error code; no cancellation test; AzurePipelinesCredential stays in the plugin) — Harden `TokenCredentials`
- **Files:** `src/Azure/Impl/TokenCredentials.cpp`, `src/Azure/Impl/BlobHelpers.cpp`
- **Problems:**
  - `SendWithRetry` ignores timer cancellation errors and does not honor `Retry-After` on a 429 response.
  - `ChainedTokenCredential` returns only the last error and logs nothing about which sources failed. The Azure SDK gave aggregated diagnostics.
  - `GetEnvironmentValue` is duplicated in `BlobHelpers.cpp` and `TokenCredentials.cpp`.
- **Required:**
  - Stop retrying when the timer is cancelled (`operation_aborted`).
  - Honor `Retry-After`, clamped to a sane maximum.
  - Have the chained credential aggregate per-source failures into the final error message and log each one at debug or warning level.
  - Move `GetEnvironmentValue` into a single shared internal helper.
  - Optional: consider moving `AzurePipelinesCredential` into `libs/AzureClient`. Note the decision.
- **Tests:** add `TokenCredentialsTests.cpp` using a fake `IHttpClient` (see `libs/AzureClient/tests` for `FakeHttpClient`). Cover: chain fallthrough, the aggregated error, `Retry-After`, and cancellation.

### [x] T09 (decision: always try managed identity, system-assigned when no id) — Document and test the credential-chain behavior change
- **File:** `src/Azure/Impl/BlobHelpers.cpp` (chained credential construction), `README.md`/docs
- **Problem:** `origin/main` effectively used a system-assigned managed identity, because the options were wrong. The branch changes the chain in two ways:
  - It uses the user-assigned client ID, and adds `ManagedIdentityCredential` only when an ID is supplied.
  - It adds the Environment and Workload credentials only when their environment variables are set.

  This silently changes behavior for some deployments.
- **Required:** decide whether a system-assigned managed identity should still be tried when no client ID is given, and implement that decision. Document the final order and conditions of the chain in the README. Add a release note.
- **Tests:** unit test of chain composition for each combination of env vars and client ID.

### [x] T10 — Retry only transient errors in `CreateIfNotExistsWithRetry`
- **File:** `src/Azure/Impl/BlobHelpers.cpp`
- **Problem:** It retries every error, including auth failures, with 2+3+4+5 s sleeps, on top of the client's own retries. Behavior predates the branch.
- **Required:** retry only transient errors (reuse T03/T04 classification); rethrow others immediately.
- **Tests:** a 403 fails immediately; a 503 retries.

---

## P2 — Performance / robustness

### [x] T11 (deviation: no batch delete in AzureClient - kept bounded concurrency; first failure now thrown; remaining counted over all pages; no unit test, container client is not mockable) — `DeleteDir` regression (Blob Batch removed)
- **File:** `src/Azure/Impl/BlobFilesystemImpl.cpp` (~L430–475)
- **Problem:** Blob Batch deletes (256 per request) were replaced by up to 256 concurrent single DELETE requests per page.
  - Failures are only logged at warning level.
  - The final "remaining blobs" check reads only the first list page.
- **Required:**
  - Restore batch delete if AzureClient supports it, or add it there.
  - Otherwise bound concurrency and return an error `Status` when any delete fails.
  - Check for remaining blobs across all pages, or with a `MaxResults=1` probe.
- **Tests:** mock test where one delete fails and `DeleteDir` returns a non-OK status.

### [x] T12 (no call-count test, container client is not mockable) — Reduce request counts in listing paths
- **File:** `src/Azure/Impl/BlobFilesystemImpl.cpp`
- **Problems:**
  - `GetChildrenFileAttributes` makes N+1 GetProperties calls.
  - The `FileExists` fallback to `GetChildren(name, 1)` keeps following continuation markers, so it costs one request per blob.

  Both behaviors predate this branch.
- **Required:**
  - List blobs with `include=metadata` and read the file-size metadata from the listing.
  - Stop the `FileExists` probe after the first result.
- **Tests:** mock call-count assertions.

### [x] T13 (TODO/backlog note only) — Avoid double-copy on download
- **Files:** `src/Azure/Impl/PageBlob.cpp`, async read path (`ReadableFileImpl::ReadAsync`)
- **Problem:** Each download lands in a `std::string Content` and is then copied into the RocksDB scratch buffer.
- **Required:** if AzureClient can write a response body into a caller-provided buffer, use that capability. If not, add a backlog item for it in AzureClient and leave a TODO. Low priority.

### [x] T14 (PEM cached once per process; still written to a short-lived temp file because HttpClientOptions only supports a CA file) — Cache the Windows root CA bundle
- **File:** `src/Azure/Impl/ClientRuntime.cpp`
- **Problem:** Every `BlobFilesystemImpl` construction enumerates the Windows ROOT certificate store and writes a temporary PEM file. The intermediate CA store is not included, and there is no revocation checking.
- **Required:**
  - Build the bundle once per process (thread-safe static) and reuse it.
  - Clean up the temp file, or load the certs directly into the `X509_STORE` and skip the file entirely (preferred).
  - Document that intermediate CAs and revocation checking are not covered.

### [x] T15 (no dedicated test; covered by existing lock tests) — Lease renewal holds the mutex during network I/O
- **File:** `src/Azure/Impl/BlobFilesystemImpl.cpp` (`RenewLease`)
- **Problem:** `m_lockFilesMutex` stays held across network calls and their retries, which blocks `LockFile`/`UnlockFile`. Existing behavior.
- **Required:** copy the set of leases while holding the lock, release it, renew outside the lock, then re-take it only to update state.

### [x] T16 (retry cap exhausted -> IOError via synthetic 412 RequestFailedException; Poll was already documented) — Async IO semantics and retry caps
- **Files:** `src/Azure/BlobFilesystem.cpp` (`Poll`), `src/Azure/AsyncReadRequest.cpp`, `src/Azure/Impl/ReadableFileImpl.cpp` (`ReadAsync`, `DownloadWithRetry`)
- **Problems:**
  - `Poll` ignores `min_completions` and waits for every handle.
  - If the blob keeps changing, the precondition-failed (412) retry loops have no upper bound.
- **Required:**
  - Document the `Poll` behavior in a code comment. It is acceptable per the RocksDB contract.
  - Cap the precondition retries at about 5 attempts, then return `IOError`/`Busy`.
- **Tests:** a mock that always returns 412 terminates with an error.

---

## P2 — Tests, docs, build

### [x] T17 (SupportedOps and UUID lease unit tests; double-Register and cache-hit added as integration tests against real Azure) — Add the missing tests
Covers the gaps not already listed under T03–T16:
- `Plugin::Register` called twice with different `io_context`s: the second registration wins, and the old context is not used.
- `BlobFilesystem::SupportedOps` now returns `1 << kAsyncIO`. On `origin/main` it returned `kAsyncIO == 0`, so async IO was effectively off. Assert the new value, and note the behavior change in the release notes.
- The `ReadAsync` cache-hit path (`TryReadFromCache`) completes without a download.
- Lease ID format (UUID) and reuse (with T04).

### [x] T18 — Version bump and changelog for the breaking API change
- **Files:** root `CMakeLists.txt` (`project(... VERSION 0.0.1)`), add/update `CHANGELOG.md`
- **Problem:** `Plugin::Register` gained a required `boost::asio::io_context&` parameter, which breaks the public API. The other behavior changes need recording too: async IO is now enabled, the credential chain changed (T09), and the SDK was replaced.
- **Required:** bump the version (at least the minor version; pre-1.0) and write a changelog entry that lists the breaking changes and migration steps.

### [x] T19 — Validate RocksDB 11.8.1 and the vcpkg baseline bump (unit 191/191 and integration 93/93 pass against a real Azure account; baseline versions and release-note review recorded in CHANGELOG.md)
- **File:** `vcpkg.json` (`builtin-baseline` `9e593bb…` → `c748cb4…`)
- **Problem:** The baseline bump upgrades every dependency (boost, openssl, gtest, …), not only RocksDB.
- **Required:**
  - List the resolved versions before and after (`vcpkg list` or the build log).
  - Check the RocksDB release notes between the old and new versions for `FileSystem`/`FSRandomAccessFile`/`SecondaryCache` API changes.
  - Run the integration tests against a real storage account.
  - Record the results so they can be included in the PR description.

### [x] T20 — Fix the installed package config (find_dependency added; install verified with --prefix; consumer smoke test in tests/package, run manually; warnings-as-errors stays off for vendored libs)
- **Files:** `src/Azure/**/aveva-rocksdb-plugin-azure-config.cmake.in`, `infrastructure/cmake/AvevaClientLibraries.cmake`, `libs/**/CMakeLists.txt`
- **Problem:**
  - The config calls `find_dependency(RocksDB)` only. It is missing the impl and core targets plus the new AzureClient, HttpClient, Boost, and OpenSSL dependencies.
  - The in-tree `http-client` shim may not survive `install()`.
  - Warnings-as-errors is disabled for `libs/`.
- **Required:** add `find_dependency` calls for every transitive public dependency and make sure the vendored libraries are installed and exported. Add a minimal consumer smoke test, e.g. a `tests/package` project that runs `find_package(aveva-rocksdb-plugin-azure)` and links. Document why warnings-as-errors is off, or turn it back on and fix the warnings.

### [x] T21 (kept PUBLIC because public headers include Boost.Asio; documented in README) — `_WIN32_WINNT` is a PUBLIC compile definition
- **File:** the CMake target that sets `_WIN32_WINNT=0x0A00`
- **Problem:** A PUBLIC definition forces the value on every consumer.
- **Required:** make it PRIVATE if no public header needs it. Otherwise document it in the README as a consumer requirement.

### [x] T22 — Formatting churn and missing trailing newlines
- **Files:** for example `include/.../BlobFilesystem.hpp` (reformatted wholesale), plus `src/Azure/BlobFilesystem.cpp`, `src/Azure/Plugin.cpp`, and `src/Azure/Impl/AzureContainerClient.hpp`, which lack a trailing newline
- **Required:** run `clang-format -i` with the repo's `.clang-format` on every touched file under `src/`, `include/`, and `tests/`. Consider restoring the original formatting on headers whose only change is whitespace, to keep the diff reviewable.

---

## Confirmed OK (no action)
- Re-registering the plugin correctly replaces the stored settings.
- `cachePath` is now an owned `std::string`, which fixes a dangling `string_view` from `origin/main`.
- `AsyncReadRequest` abort/delete: once aborted, the scratch buffer is never written, and a `shared_ptr` keeps the file alive.
- `RenameFile` behavior matches `origin/main`.
- `BlobFilesystemImpl` member destruction order is safe: the `jthread` is declared last and the runtime before the clients.
- TLS: peer verification, host-name verification, and SNI are configured in `libs/HttpClient` (`RequestOperation.hpp`, `TlsContextConfigurator.cpp`), with a minimum of TLS 1.2.

## Definition of done
- [x] All P0 tasks resolved, or explicitly deferred with the user's sign-off.
- [x] All P1 tasks done, with tests.
- [x] Debug build passes; `ctest ... -E Integration` all green (191/191); integration tests 93/93 pass against real Azure.
- [x] `clang-format` clean; README/CHANGELOG updated.
- [ ] PR description follows `.github/PULL_REQUEST_TEMPLATE.md`. **The user approves it before the PR is created.**
