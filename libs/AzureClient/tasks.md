# aveva-azure-client — Task Backlog

Goal: a **performant, modern C++23 / Boost.Asio Azure Blob client** with **high, meaningful test coverage**.

## Status (2026-10-02 review)

All earlier backlogs (R01–R06, T01–T075, A01–A15, T020 file split) are done; see `CHANGELOG.md` `[Unreleased]` and
git history. Baseline at commit `6cf1683`: `cmake --build --preset WindowsDebug` builds with zero warnings and
`ctest --preset WindowsDebug -L unit` passes **485/485**.

This file lists the findings of a fresh code review (tasks `B01`–`B17`). Work them roughly in priority order
(P1 → P3). Each task is self-contained: it names the files, the problem, the required change and how to verify it.
Do one task per commit, message prefix like `fix: ... (B01)`.

**Definition of done for every task**

1. `cmake --build --preset WindowsDebug` — zero warnings.
2. `ctest --preset WindowsDebug -L unit` — all pass (count goes up when tests are added).
3. New/changed behaviour is covered by a unit test using `tests/FakeHttpClient.hpp` / `tests/TestFixtures.hpp`
   (new test files must be added to `tests/CMakeLists.txt`).
4. A `CHANGELOG.md` → `[Unreleased]` line for every user-visible change (Added/Changed/Fixed/Breaking).
5. Changed files are clang-formatted with the repo `.clang-format` (see B08).

---

## P1 — correctness / security

### B01 — Block IDs collide across concurrent uploads to the same blob

- **Files:** `src/BlockBlobClient.cpp` (`UploadFromOperation::StageBlock`), `src/BlobModels.cpp`
  (`Models::EncodeBlockId`), `include/AVEVA/AzureClient/Models/BlobModels.hpp`.
- **Problem:** `UploadFromOperation` names blocks `EncodeBlockId(index)` = base64 of `"%016u"` of the index. Every
  upload of the same blob uses the *same* IDs (`0000000000000000`, `0000000000000001`, …). Two concurrent
  `UploadFromFileAsync`/`UploadFromStreamAsync` calls to one blob (or a retry overlapping an abandoned upload)
  overwrite each other's uncommitted blocks, so a successful `Put Block List` can commit a mix of both sources
  (silent data corruption). Azure SDKs use a per-upload random prefix for this reason.
- **Fix:** Generate a per-operation random prefix once in the `UploadFromOperation` constructor (e.g. 16–32 hex chars
  from `Private::CreateClientRequestId()` with dashes removed) and build each ID as
  `base64(prefix + zero-padded index)`. All IDs of one upload must have equal encoded length and decode to ≤ 64 bytes
  (`ValidateBlockId`). Keep public `Models::EncodeBlockId(std::uint64_t)` unchanged for compatibility; add an internal
  helper (e.g. `Private::EncodeUploadBlockId(std::string_view prefix, std::uint64_t index)`).
- **Tests:** two `UploadFromStreamAsync` operations against one `FakeHttpClient` produce disjoint `blockid=` query
  values; all IDs within one upload have identical length; the committed `<BlockList>` body lists exactly the IDs
  staged by that operation, in order. Update any existing test that hard-codes `EncodeBlockId(0)`-style IDs for
  `UploadFrom*` (grep `EncodeBlockId` in `tests/`).

### B02 — Bearer tokens may be sent over plain `http://`

- **Files:** `src/BlobRequestHelpers.cpp` (`ValidateEndpoint`, `ValidateConnectionOptions`, `BuildConnection`).
- **Problem:** `ValidateEndpoint` accepts `http` and `https` for any credential. With `TokenCredential` or
  `BearerToken` an `http://` endpoint sends `Authorization: Bearer …` in clear text (Azure SDKs refuse this).
  SharedKey over http only leaks a per-request signature, SAS over http is the caller's explicit choice.
- **Fix:** In `ValidateConnectionOptions`, if the scheme is `http` (case-insensitive) and `TokenCredential` or
  `BearerToken` is set, throw `std::invalid_argument("Token credentials require an https ServiceEndpoint.")` —
  **unless** the host is loopback (`localhost`, `127.0.0.1`, `[::1]`) so Azurite/local proxies keep working.
  Document the rule on the `TokenCredential`/`BearerToken` members of the three options structs
  (`BlobClientOptions.hpp`, `BlobContainerClient.hpp`, `BlobServiceClient.hpp`). CHANGELOG: **Breaking**.
- **Tests:** add cases to `tests/OptionValidationTests.cpp` (parameterized matrix): http+token → throws;
  http+bearer → throws; http://127.0.0.1:10000+token → ok; https+token → ok; http+SharedKey/SAS → ok.

### B03 — No CR/LF/NUL validation of caller-supplied header values

- **Files:** `src/BlobRequestHelpers.cpp` (`SendAuthorizedRequestAsync`, `ApplyMetadata`), possibly
  `src/Credentials.cpp`.
- **Problem:** Caller strings go straight into request headers: metadata values, `BlobHttpHeaders` fields, lease IDs,
  `ProposedLeaseId`, `IfMatch`/`IfNoneMatch`, copy source URI, `SourceContentMd5`, transactional hashes, API version,
  `ManagedIdentityCredentialOptions::IdentityHeader`. A value containing `\r`/`\n` enables header injection unless the
  transport rejects it. `aveva-http-client`'s `HttpHeader` does no validation (see
  `build/WindowsDebug/vcpkg_installed/x64-windows-static-md/include/AVEVA/HttpClient/HttpHeader.hpp`).
- **Fix:** First write a quick test against the real transport (or read the aveva-http-client source in the vcpkg
  buildtree) to see whether it rejects such values with `HttpClientError::InvalidRequest`. Regardless, add a
  defence-in-depth check in `SendAuthorizedRequestAsync` (it already loops over headers to validate metadata names):
  reject any header whose name or value contains `\r`, `\n` or `\0` by posting
  `std::errc::invalid_argument` via `PostCompletion` (never complete inline). Apply the same check to the token
  requests built in `src/Credentials.cpp` (fail via the existing `Fail(...)` helper).
- **Tests:** metadata value `"a\r\nx-ms-evil: 1"` → `invalid_argument`, no request reaches `FakeHttpClient`;
  same for `BlobHttpHeaders::ContentType`, a lease ID and `IdentityHeader`.

### B04 — `UploadFrom*` can throw out of the async API / from io_context::run

- **Files:** `src/BlockBlobClient.cpp` (`UploadFromOperation::StartLocked`, `IssueMore`, `StageBlock`, `Commit`,
  `OnCommitComplete`, `UploadFromFileAsyncImpl`, `UploadFromStreamAsyncImpl`); `src/BlobDownload.cpp`
  (`MakeFailureFromCurrentException`).
- **Problem:**
  1. `std::make_unique<UploadBuffer>(m_blockSize)` / `UploadBuffer(*m_knownSize)` can throw `std::bad_alloc` (huge
     `BlockSize`, huge `SingleUploadThreshold`), and `BuildBlobRequest`/`BuildStageBlockRequest` can throw. In
     `Start()` this escapes the initiating function; in `IssueMore()` (run from `OnStageComplete` on the strand) it
     escapes into `io_context::run()` and leaves the operation hung with `m_mutex` state inconsistent. The download
     engine already maps every exception to an error (`MakeFailureFromCurrentException`) — upload does not.
  2. `UploadFromOptions::BlockSize` is only checked for `0`. Values > `BlobTransferLimits::MaxStageBlockBytes`
     (4000 MiB) are sent and rejected by the service only after reading/allocating a huge buffer.
  3. `OnCommitComplete` maps a header-parse failure to `std::errc::invalid_argument`; it should be
     `BlobStorageErrorCode::InvalidResponse` via `Private::MakeInvalidResponseFailure` (as `CompleteParsed` does).
- **Fix:** Move `MakeFailureFromCurrentException` into `BlobRequestHelpers.{hpp,cpp}` (shared). Wrap the bodies of
  `StartLocked`, `IssueMore` and `Commit` in `try { … } catch (...) { Fail(MakeFailureFromCurrentException()); }`
  (in `StartLocked` use `FailEarly` semantics so completion is posted, never inline). Validate
  `BlockSize <= MaxStageBlockBytes` up front in both `UploadFrom*AsyncImpl` (post `invalid_argument` with a message).
  Fix the commit parse error code. Update the `UploadFromOptions` doc comment in `BlobOperationOptions.hpp`.
- **Tests:** `BlockSize = MaxStageBlockBytes + 1` → `invalid_argument` without any request; `BlockSize =
  std::numeric_limits<std::size_t>::max() / 2` on a stream → completes with `not_enough_memory` or
  `invalid_argument` (no throw); commit response with a malformed `Last-Modified` → `InvalidResponse`.

### B05 — Download treats a `200` reply to a ranged probe as data at the requested offset

- **Files:** `src/BlobDownload.cpp` (`DownloadOperation::OnProbe`).
- **Problem:** When the probe (ranged GET at `m_begin`) gets a non-206 status, the body is assumed to start at
  `m_begin` (`m_end = m_begin + body.size(); Deliver(m_begin, …)`). A 200 means the server/proxy ignored `Range` and
  returned the *whole blob from byte 0*. With `Range.Offset > 0` this silently returns the wrong bytes; with
  `Range.Length` set it returns more bytes than requested.
- **Fix:** On a 200 to the probe: if `m_begin != 0` → `Fail(MakeInvalidResponse("The service ignored the requested
  range."))`. If `m_begin == 0` and `m_end` is set, deliver only the first `*m_end` bytes (`body.resize`). Otherwise
  keep current behaviour (whole blob, unranged).
- **Tests (`tests/ParallelDownloadTests.cpp` or `DownloadToFileTests.cpp`):** fake returns 200 + full body for
  `Range{Offset=10}` → `InvalidResponse`; for `Range{Offset=0, Length=5}` on a 20-byte body → exactly 5 bytes.

### B06 — Unbounded `expires_on` in token responses can overflow `system_clock`

- **Files:** `src/Credentials.cpp` (`ParseTokenResponse`).
- **Problem:** `expires_in` is bounded to [1 s, 24 h] but the fallback `expires_on` is only checked `>= 0`.
  `Clock::time_point{std::chrono::seconds{*expiresOn}}` overflows (UB) for values above ~9.2e11 s on MSVC (100 ns
  ticks) / ~9.2e9 s on libstdc++ (1 ns ticks); a past value yields an already-expired token and a refresh on every
  request. The comment above `MinTokenLifetimeSeconds` claims hostile values are handled.
- **Fix:** Compute `lifetime = expiresOn - now_in_seconds` and accept only `MinTokenLifetimeSeconds <= lifetime <=
  MaxTokenLifetimeSeconds` (else `InvalidResponse`), then construct the time point from `now + lifetime`.
- **Tests (`tests/CredentialsTests.cpp`):** `expires_on` = `INT64_MAX`, `0`, `now-10`, `now+3600` → invalid,
  invalid, invalid, valid.

### B07 — Control characters in a source comment

- **Files:** `src/BlobRequestHelpers.hpp` lines 253–254.
- **Problem:** The comment contains a vertical tab (`\v`) and bell (`\a`) where `` `value` `` / `` `allowEmpty` ``
  were intended (a `\value`/`\allowEmpty` escape was interpreted, leaving "unless <VT>alue is canonical…",
"only when <BEL>llowEmpty is set"). Verify with
  `git grep -nP "[\x00-\x08\x0b\x0c\x0e-\x1f]"`.
- **Fix:** Replace with `` `value` `` and `` `allowEmpty` ``. While there, make the error messages thrown by
  `ValidateBase64AndGetDecodedLength` (in `src/BlobRequestHelpers.cpp`) generic ("must be a valid base64 string")
  instead of always saying `SharedKey.AccountKey` — the function also validates block IDs and user-delegation keys.
  Keep the messages that existing tests assert on, or update those tests.
- **Acceptance:** the `git grep` above returns nothing.

### B08 — Formatting has drifted; no CI format gate

- **Files:** whole tree; `azure-pipelines.yml`; `CONTRIBUTING.md`.
- **Problem:** `clang-format --dry-run` reports violations in 42 files (e.g. `src/RequestPipeline.cpp:95`
  constructor initializer on one long line, `src/SharedKeySigner.cpp` mis-indented `struct ParsedUrl` and
  `EstimatedCanonicalizedHeaderLength`, `include/AVEVA/AzureClient/Detail/AsyncInitiation.hpp` 52 issues, several
  test files with 40–80 issues). Nothing in CI checks formatting.
- **Fix:**
  1. Decide the pinned clang-format major version (CI's `LinuxDebugLlvm` uses LLVM ≥ 19; local tooling here is
     22.1.3). Pick one, record it in `CONTRIBUTING.md`, and confirm `.clang-format` options are valid for it.
  2. Run `clang-format -i` on all `*.cpp/*.hpp` under `src include tests benchmarks examples` in a **separate,
     formatting-only commit** (add its hash to a new `.git-blame-ignore-revs`).
  3. Add a CI step (Linux job in `azure-pipelines.yml`) running
     `git ls-files '*.cpp' '*.hpp' | xargs clang-format-<N> --dry-run --Werror`.
- **Acceptance:** dry-run reports zero issues; build and unit tests unchanged.

---

## P2 — robustness / spec conformance

### B09 — SharedKey canonicalization ignores repeated query parameters and headers

- **Files:** `src/SharedKeySigner.cpp` (`ParseUrl`, `BuildSharedKeyStringToSign`).
- **Problem:** Per the Azure spec, a query parameter with several values is canonicalized as one line
  `name:v1,v2` (values sorted), and repeated `x-ms-*` headers as one `name:v1,v2` line. The code emits one line per
  occurrence. Not hit by current request builders (list `include` is comma-joined), but any future request with a
  repeated key will fail authentication with 403.
- **Fix:** After lower-casing and sorting, group consecutive equal names and join their (sorted) values with `,`;
  same for `x-ms-` headers. Keep the single-buffer approach.
- **Tests (`tests/BlobRequestHelpersTests.cpp`):** URL `…?comp=list&include=b&include=a` → canonical resource ends
  `\ncomp:list\ninclude:a,b`; two `x-ms-meta-x` headers → one joined line.

### B10 — `Retry-After` hint is clamped to `MaxDelay`, contradicting the docs

- **Files:** `src/RequestPipeline.cpp` (`OnAttemptCompleteOnStrand`), `include/AVEVA/AzureClient/BlobClientOptions.hpp`.
- **Problem:** Docs say a server `Retry-After`/`x-ms-retry-after-ms` hint "takes precedence", but the code clamps it
  to `MaxDelay`, so a throttled client can retry earlier than the server asked (more 429/503s).
- **Fix (pick one, document it):** preferred — honour the hint up to the existing 24 h `ParseRetryAfter` ceiling and
  only clamp computed backoff to `MaxDelay`; alternatively keep the clamp and correct the comment. Update
  `tests/RetryTests.cpp` accordingly.

### B11 — Lease operations accept an empty lease ID

- **Files:** `src/BlobRequestHelpers.hpp` (`RenewLeaseAsync`, `ChangeLeaseAsync`, `ReleaseLeaseAsync`).
- **Problem:** Renew/Change send an empty `x-ms-lease-id` header (`AddHeader`), Release omits it
  (`AddHeaderIfNotEmpty`); Change also allows an empty `ProposedLeaseId`. The service answers 400, costing a round
  trip, and the behaviour is inconsistent with `AbortCopyBlobFromUriAsync`, which validates its ID client-side.
- **Fix:** Post `invalid_argument` ("LeaseId must not be empty." / "ProposedLeaseId must not be empty.") via
  `PostCompletion` before building the request, for both blob and container targets.
- **Tests:** `tests/LeaseTests.cpp` — each of the three operations with empty ID → `invalid_argument`, no request sent.

### B12 — Token acquisition has no retry for transient failures

- **Files:** `src/Credentials.cpp` (`SendTokenRequest`, `ParseTokenResponse`).
- **Problem:** Token requests call `IHttpClient::SendAsync` directly: one timeout, 429 or 5xx from Entra ID / IMDS
  fails the storage operation. IMDS documents retrying 404/410/429/5xx. All non-2xx statuses map to
  `AuthenticationFailed`, losing the distinction between "bad credentials" and "try again".
- **Fix:** Route token requests through `Private::SendWithRetryAsync` with `RequestAuth{}` (Kind::None) and a
  `RetryOptions` member on each credential's options struct (default = `RetryOptions{}`); for
  `ManagedIdentityCredential` also treat 404 and 410 as retriable (wrap `IsRetriableFailure` or add a predicate
  parameter). Map 429/5xx after exhaustion to the transport/status error rather than `AuthenticationFailed`.
  Never log or surface the request body (`Redaction.hpp`).
- **Tests:** fake returns 503 then 200 → success after one retry; 400 → `AuthenticationFailed`, no retry.

### B13 — Download/upload do blocking file & stream I/O on the HTTP executor

- **Files:** `src/BlobDownload.cpp` (`StreamSink`/`FileSink::Write`), `src/BlockBlobClient.cpp`
  (`ReadInto` in `StartLocked`/`IssueMore`).
- **Problem:** `std::ostream::write`/`std::istream::read` run inside transport completions, i.e. on the threads that
  drive all HTTP I/O. A slow disk or a user stream that blocks stalls every other in-flight request on that
  `io_context`, and limits download throughput to one writer.
- **Fix (minimum):** document it on `DownloadToAsync`/`DownloadToFileAsync`/`UploadFrom*Async` in the public
  headers ("stream I/O runs on the client's executor; use a dedicated io_context or a fast stream").
  **Optional (measure first with `benchmarks/UploadThroughputBenchmarks.cpp`):** add an optional
  `std::optional<boost::asio::any_io_executor> IoExecutor` to `DownloadToOptions`/`UploadFromOptions` and post
  reads/writes there, re-entering the operation's queue/strand on completion.

### B14 — Cancellation slot is cleared from arbitrary threads

- **Files:** `src/RequestPipeline.cpp` (`RetryOperation::Finish`), `src/BlobDownload.cpp` (`Finish`),
  `src/BlockBlobClient.cpp` (`UploadFromOperation::Finish`); thread-safety docs in `README.md` / public headers.
- **Problem:** Asio cancellation slots are not thread-safe: `slot.clear()`/`assign()` must not race with
  `signal.emit()`. `Finish` clears the parent slot on whatever thread completes the operation (transport thread),
  while the caller may emit from its own thread. This is inherent to Asio, but the contract is not documented.
- **Fix:** Document in the thread-safety section that a cancellation signal must be emitted from the same executor
  (or strand) the completion handler runs on. Add a test in `tests/AsyncBehaviorTests.cpp` that emits via
  `boost::asio::post(handlerExecutor, …)` to show the supported pattern. No behaviour change.

---

## P3 — performance / polish

### B15 — Avoid copying caller buffers on `*Bytes` uploads

- **Files:** `src/BlockBlobClient.cpp` (`UploadBytesAsyncImpl`, `StageBlockBytesAsyncImpl`),
  `src/AppendBlobClient.cpp` (`AppendBlockBytesAsyncImpl`), `src/PageBlobClient.cpp` (equivalent).
- **Problem:** `request.SetBody(Private::BytesToString(content))` copies the whole payload (up to 5000 MiB for Put
  Blob) because the async API cannot assume the span outlives the operation.
- **Fix:** Measure first (`benchmarks/UploadThroughputBenchmarks.cpp`). If worthwhile, add overloads taking
  `std::shared_ptr<const std::vector<std::byte>>` (or `std::vector<std::byte>&&`) that keep the buffer alive in the
  completion and use `SetBodyView`. Do not change the existing span overloads' semantics. Document in CHANGELOG
  (Added) and `tests/PublicApiCoverage.md`.

### B16 — Redaction misses SAS signatures inside header values

- **Files:** `src/Redaction.hpp`, `tests/RedactionTests.cpp`.
- **Problem:** `RedactHeaderForDiagnostics` returns `x-ms-copy-source` (a URL that usually carries a SAS `sig=`)
  unchanged. Nothing logs today, but the helper is the mandated gate for future logging.
- **Fix:** For `x-ms-copy-source` (and any header value that starts with `http://`/`https://`), return
  `RedactUrlForDiagnostics(value)`. Also redact the `sig` parameter case-insensitively when percent-encoded
  (`%73ig` is out of scope; just document). Add tests.

### B17 — Carry-over follow-ups (from the previous backlog, still open)

- **clang-tidy backlog** (non-gating in `.clang-tidy`): `misc-include-cleaner`, magic numbers, identifier naming,
  `modernize-use-designated-initializers`, `bugprone-unchecked-optional-access`,
  `cppcoreguidelines-pro-bounds-avoid-unchecked-container-access`, `performance-move-const-arg` /
  `unnecessary-value-param`. Fix one check at a time and promote it to `WarningsAsErrors`.
  Noticeable include hygiene issues seen in review: `src/BlobRequestHelpers.cpp` still includes libxml2 and OpenSSL
    headers (lines 38–56) that are likely unused after the T020 split — remove any that are not needed; `src/BlobRequestHelpers.hpp` (~1100 lines) includes every public
  header — consider splitting the per-operation templates into an `Operations.hpp` so translation units pull in less.
- **Linux clang-tidy:** `LinuxDebugLlvm` needs clang/clang-tidy ≥ 19 with libstdc++ (clang 18's `__cpp_concepts`
  blocks `std::expected`), or libc++.
- **Fuzzing in CI:** `AVEVA_AZURE_CLIENT_FUZZ` builds the libFuzzer target (Clang only); keep the scheduled
  time-boxed run with a persisted corpus healthy, and add a fuzz target for `ParseTokenResponse` (JSON) and
  `ParseContentRange`/`ParseHttpDateHeader`.

---

## Conventions

- PascalCase functions/public members, `m_camelCase` private members, `camelBack` locals/params, 4-space indent,
  Allman braces, `.clang-format` in repo root. Internal helpers live in `AVEVA::AzureClient::Private`.
- Unit tests use GoogleTest + `tests/FakeHttpClient.hpp` and `tests/TestFixtures.hpp`; add new files to
  `tests/CMakeLists.txt`.
- Add a line to `CHANGELOG.md` → `[Unreleased]` for every user-visible change (Added/Changed/Fixed/Breaking).
- Never complete synchronously: every early-exit error must use `Private::PostCompletion`.
- The async API must never throw after argument binding; map exceptions to `BlobStorageError`.
