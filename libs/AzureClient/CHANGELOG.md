# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
for the package version published in `vcpkg.json`.

## [Unreleased]

### Added
- Fuzz harness coverage of the token-response JSON parser (via `ManagedIdentityCredential`); `ParseContentRange` / `ParseHttpDateHeader` were already fuzzed. Removed unused libxml2/OpenSSL/`<mutex>`/`<cstring>` includes from `BlobRequestHelpers.cpp`. Still open from B17: promoting the remaining clang-tidy checks to errors, splitting `BlobRequestHelpers.hpp`, and Linux clang-tidy with clang >= 19. (B17)
- `BlockBlobClient::UploadAsync(std::shared_ptr<const std::vector<std::byte>>, ...)`: zero-copy Put Blob that shares ownership of the buffer until completion (the span overloads still copy). Only Put Blob gets the overload; the Stage/Append/Page byte paths are unchanged. (B15)
- `WindowsDebugLlvm` (clang-cl via Ninja) and `LinuxDebugLlvm` (clang) CMake presets, which hard-code `ENABLE_CLANG_TIDY=ON` so clang-tidy runs on every compile instead of as a separate, manually-invoked step.
- Async-behaviour matrix tests: explicit `deferred` tokens, cancelling a `co_spawn`ed `use_awaitable` operation, signals emitted before initiation, cancellation during token acquisition, mid-flight cancellation for container/service clients, and destroying the `io_context` with pending transport, posted-completion, retry-backoff, multi-block upload and download work.
- Parser robustness tests (`tests/ParserRobustnessTests.cpp`) covering malformed, truncated, random, namespaced, entity/CDATA and very large response bodies, plus date and `Content-Range` edge cases.
- A libFuzzer harness for the response parsers (`tests/fuzz/ParseXmlFuzz.cpp`), built with the opt-in `AVEVA_AZURE_CLIENT_FUZZ` CMake option (Clang only); its seeds are replayed by the unit tests on every build.
- `tests/RequestShapeTests.cpp`: one parameterised row per public operation (64 rows, including snapshot- and version-scoped clients) asserting the HTTP method, path, exact query parameters with the SAS last, the common `x-ms-version`/`x-ms-date`/`x-ms-client-request-id` headers, operation-specific headers, the `BlobRequestConditions` header mapping, metadata headers and body (T27).
- Metadata names are validated client-side as C# identifiers (`[A-Za-z_][A-Za-z0-9_]*`). Any request carrying an invalid `x-ms-meta-*` name fails with `BlobStorageErrorCode::InvalidMetadata` (== `std::errc::invalid_argument`) without being sent; the same code is mapped from the service's `InvalidMetadata` error (T24).
- Entra ID credentials built on `IHttpClient` (new header `Credentials.hpp`): `ClientSecretCredential`, `WorkloadIdentityCredential` (federated token file, `FromEnvironment()`) and `ManagedIdentityCredential` (IMDS, or the App Service identity endpoint via `FromEnvironment()`). The integration test helpers now acquire tokens through `ClientSecretCredential`.
- `BlobClient::GetTagsAsync`/`SetTagsAsync` (with client-side tag validation and `x-ms-if-tags`), `BlobContainerClient::FindBlobsByTagsAsync` and `BlobServiceClient::FindBlobsByTagsAsync`.
- `BlobClient::UndeleteAsync`.
- Blob versions: `BlobClientOptions::VersionId` (`?versionid=`, mutually exclusive with `Snapshot`) and `BlobClient::WithSnapshot`/`WithVersionId` for snapshot- or version-scoped clients.
- `Sas::BlobSasBuilder` (container/blob/snapshot/version service SAS, signed with the account key or a user delegation key) and `Sas::AccountSasBuilder`, producing `sv=2023-11-03` query strings usable as `SasToken`. Signatures are verified against the Azure Python SDK.
- `BlobServiceClient::GetPropertiesAsync` (`Models::BlobServiceProperties`), `GetAccountInfoAsync` (`Models::AccountInfo`) and `GetUserDelegationKeyAsync` (`Models::UserDelegationKey`) in the new public header `Models/BlobServiceModels.hpp` (T23).
- `BlobContainerClient::ListBlobsAllAsync` and `BlobServiceClient::ListBlobContainersAllAsync`: follow `NextMarker` and complete once with every page merged (any completion token; repeated markers are rejected) (T23).
- `ListBlobsOptions` include flags (`IncludeSnapshots`, `IncludeVersions`, `IncludeDeleted`, `IncludeTags`, `IncludeUncommittedBlobs`, `IncludeCopy`); `BlobItem::Deleted` and `BlobItem::Tags` (`Models::BlobTags`) (T23).
- Transactional `Content-MD5`/`x-ms-content-crc64` hashes on `UploadBlockBlobOptions` (Upload/StageBlock) and `AppendBlockOptions`; `StageBlockResult::ContentMd5` (T23).
- `BlockBlobClient::StageBlockFromUriAsync` (Put Block From URL with source range/MD5), `BlobClient::CopyFromUriAsync` (synchronous Copy Blob From URL) and `BlobClient::AbortCopyFromUriAsync` (T23).
- `AppendBlobClient::SealAsync`, `CreateIfNotExistsAsync`, and `AppendBlockOptions::IfAppendPositionEqual`/`IfMaxSizeLessThanOrEqual`; `AppendBlockResult` now reports `AppendOffset` and `CommittedBlockCount` (T23).
- `BlobClient::RenewLeaseAsync`/`ChangeLeaseAsync` and container leases on `BlobContainerClient` (`AcquireLeaseAsync`, `RenewLeaseAsync`, `ChangeLeaseAsync`, `ReleaseLeaseAsync`, `BreakLeaseAsync`) with new `RenewLeaseOptions`/`ChangeLeaseOptions` and `RenewBlobLeaseResult`/`ChangeBlobLeaseResult` models (T23).
- `Models::BlobHttpHeaders` gains `ContentEncoding`, `ContentLanguage` and `ContentDisposition`. They are sent on upload/create (`x-ms-blob-content-*`) and on Set HTTP Headers, and parsed from Get Properties and List Blobs. (T22)
- `Models::BlobProperties` gains `ContentEncoding/Language/Disposition`, `CreatedOn`, `AccessTier`, `AccessTierInferred`, `LeaseStatus/LeaseState/LeaseDuration`, `CopyId/CopyStatus/CopySource/CopyProgress`, `ServerEncrypted`, `VersionId`, `IsCurrentVersion`, `CommittedBlockCount` (append) and `SequenceNumber` (page), from both Get Properties headers and List Blobs XML. A malformed value fails with `InvalidResponse`. (T22)
- `Models::AccessTier`, `Models::DeleteSnapshotsOption` and `Models::CopyStatus` extensible enums: named accessors (e.g. `AccessTier::Cool()`, `DeleteSnapshotsOption::OnlySnapshots()`, `CopyStatus::Pending()`), but any service value round-trips unchanged. They convert implicitly from strings, so `options.AccessTier = "Hot"` still compiles. (T22)
- `ResizePageBlobOptions` and `PageBlobClient::ResizeAsync(newSize, ResizePageBlobOptions, ...)`. The `CreatePageBlobOptions` overload is `[[deprecated]]`; only its `Conditions` were ever used. (T22)
- `WithRequestOptions(options, token = deferred)` completion-token adapter (`<AVEVA/AzureClient/WithRequestOptions.hpp>`). It sets per-call `HttpRequestOptions` while the token stays last, and it also works with the defaulted token, e.g. `co_await client.DeleteAsync(WithRequestOptions(opts))`. (T19)
- `GetDefaultRequestOptions()` on `BlobClient`, `BlobContainerClient` and `BlobServiceClient`. (T19)
- `BlobClient`: a public base class holding the operations common to every blob type (Download, DownloadTo, Delete, DeleteIfExists, GetProperties, Exists, SetMetadata, SetHttpHeaders, SetAccessTier, StartCopyFromUri, Snapshot, Acquire/Release/BreakLease), and `BlobContainerClient::GetBlobClient` returning one that shares the container's connection. `AppendBlobClient` thereby gains SetMetadata, SetHttpHeaders, SetAccessTier, StartCopyFromUri, Snapshot, the lease operations, Exists and DeleteIfExists. (T20)
- ThreadSanitizer support: `ENABLE_TSAN` CMake option, `LinuxDebugTSan`/`CILinuxDebugTSan` configure/build/test/workflow presets, and a `tsan` entry in the pipeline's sanitizer matrix. The new `aveva-azure-client-concurrency-tests` executable (label `concurrency`) runs chunked UploadFrom/DownloadTo, retries and many-thread initiation against a 4-thread `boost::asio::thread_pool` executor. (T30)
- `aveva-azure-client-public-api-tests`: a test executable that sees only the installed public headers and drives every client with a lambda, `boost::asio::use_future`, and the default deferred token inside a coroutine, so a public header that depends on a private one fails the build. (T01)
- `BlobStorageErrorCode` gained 18 common service codes (`LeaseIdMissing`, `LeaseAlreadyPresent`, `LeaseLost`, `ServerBusy`, `OperationTimedOut`, `InternalError`, `InvalidRange`, `TargetConditionNotMet`, `AppendPositionConditionNotMet`, `MaxBlobSizeConditionNotMet`, `BlobArchived`, `Md5Mismatch`, `InvalidHeaderValue`, `InvalidQueryParameterValue`, `RequestBodyTooLarge`, `BlobTierInadequateForContentLength`, and two lease-mismatch codes) and `InvalidResponse`. (T21)
- The blob-storage error category maps codes onto portable conditions: not-found codes compare equal to `std::errc::no_such_file_or_directory`, already-exists codes to `std::errc::file_exists`, auth failures to `std::errc::permission_denied`, `OperationTimedOut` to `std::errc::timed_out`, and `InvalidResponse` to `std::errc::bad_message`. (T21)
- `IsTransient(const BlobStorageError&)` reports whether a failure is retriable, using the same classification as the retry policy. (T21)
- `Response<T>::RawResponse() &&` and `Response<T>::Error() &&` rvalue-qualified accessors that move
  the raw HTTP response/error out instead of copying, used at internal call sites that forward a
  discarded `Response<T>` into a new one (existing `const&` overloads are unchanged)
- ParseContentRange and unit tests covering edge cases (bytes start-end/total, bytes */total, malformed inputs) and added DownloadToOptions.DefaultRequestOptions wiring. (T05)
- Root project documentation: `README.md`, `CONTRIBUTING.md`, and a repository `LICENSE`
- Azure DevOps pipeline scaffolding, shared pipeline variables, and a preset-driven build script
- Windows ASan triplet support, Linux UBSan presets, optional coverage instrumentation, and optional Google Benchmark targets
- Repository-wide `.clang-tidy` configuration and CI lint wiring
- `aveva-http-client` bump: `IHttpClient::get_executor()`/`executor_type`, and a template
  `SendAsync(request, options, token)` overload that accepts any Boost.Asio completion token
  (`use_future`, `use_awaitable`, `bind_executor`, etc.) and genuinely propagates its associated
  executor/allocator/cancellation slot, plus `HttpResponse::GetHeaders() &&`/`GetBody() &&`
  move accessors and `HttpRequest::ReserveHeaders()`
- Every blob client (`BlockBlobClient`, `PageBlobClient`, `AppendBlobClient`, `BlobContainerClient`,
  `BlobServiceClient`) now exposes `executor_type`/`get_executor()`, mirroring `IHttpClient`
- `UploadFromOptions::SingleUploadThreshold`: file uploads of at most this size are sent as a single
  Put Blob (default 0: only sources that fit in one block). (T10)
- `DownloadToAsync(std::ostream&, DownloadToOptions, ...)` and `DownloadToAsync(const std::filesystem::path&,
  DownloadToOptions, ...)` on `BlockBlobClient`, `PageBlobClient`, and `AppendBlobClient`, exposing
  `ChunkSize` and `Concurrency` for chunked, parallel downloads. (T11)

### Changed
- Documented that stream/file I/O of `DownloadToAsync`/`UploadFromAsync` runs on the client's executor and that a cancellation signal must be emitted from the handler's executor/strand (README, public headers); added a test of the supported pattern. (B13, B14)
- Token requests of `ClientSecretCredential`, `WorkloadIdentityCredential` and `ManagedIdentityCredential` now go through the shared retry pipeline (new `Retry` option, default 3 retries on 408/429/5xx and transport errors; managed identity also retries 404/410). 4xx authentication failures are not retried. (B12)
- XML responses are now parsed with libxml2 (vcpkg `libxml2`, no default features; a new PRIVATE `LibXml2::LibXml2` link dependency that the installed config `find_dependency`s) instead of the rapidxml copy in Boost's `property_tree::detail` namespace. Parsing is now strict: mismatched end tags, undeclared entities and any `<!DOCTYPE>` are rejected as `InvalidResponse`, and libxml2's default nesting-depth limit turns pathologically deep documents into `InvalidResponse` instead of a stack overflow. Network access, entity substitution, DTD loading and `XML_PARSE_HUGE` stay off. Text, CDATA, entity, namespace (including unbound prefixes) and `Encoded` name handling is unchanged. `BM_ParseListBlobsResultXml5000` measured 62 ms with rapidxml and 74 ms with libxml2 in Release on the same machine.
- clang-tidy now fails the lint stage for all `bugprone-*` and `performance-*` checks (except a documented few with large backlogs or ABI impact), plus `cppcoreguidelines-pro-type-member-init`, `misc-unused-parameters`, `misc-unused-using-decls` and `modernize-use-override`; the existing findings for those checks were fixed or, where intentional, suppressed with a reason.
- The CI coverage gate is now 90% lines / 80% branches, measured with `gcovr --merge-lines` so template instantiations of the completion-token machinery are counted once per source line.
- An empty response body, or an XML document whose root element is not the one the operation expects, is now reported as `BlobStorageErrorCode::InvalidResponse` instead of being parsed as an empty successful result.
- README documents defaults and limits, parallel transfers, the retry policy, path-style endpoints and the `std::span` initiation-time lifetime rule, and lists the T23 features. The `BlobContainerClient`/`BlobServiceClient` class comments are trimmed to the concise form used by `BlobClient`, without internal task references (T33).
- Documented `Response<T>::Error()`: it is engaged only when an expected service error was deliberately treated as success (`*IfExists`/`CreateIfNotExists`) (T24).
- Internal: page-write range header construction returns `std::expected`, and the duplicated ETag/Last-Modified parsing is consolidated into one helper (T24).
- `ReleaseLeaseOptions` and `BreakLeaseOptions` gain `Conditions`; lease operations now send their If-* request conditions (T23/T24).
- The trailing `requestOptions` parameter of every `...Async` operation is now `std::optional<HttpRequestOptions>` (default `std::nullopt` = use the client's `DefaultRequestOptions`). Passing an `HttpRequestOptions` still compiles unchanged. Precedence: `WithRequestOptions` > trailing argument > client default. The trailing form is kept rather than deprecated because `WithRequestOptions` already covers the token-last case. (T19)
- `BlobContainerClient` and `BlobServiceClient` operations are one-line `Private::InitiateClientOperation` calls, like the blob clients (completing the T20 refactor). `BlobContainerClient::CreateIfNotExistsAsync`/`DeleteIfExistsAsync` no-options overloads are now constrained like the others. (T19)
- Client-side `BlobStorageError`s are built by a single internal `MakeClientError` helper instead of partial designated initializers, which Clang reports as `-Wmissing-designated-field-initializers` under `-Wextra`. Removed the unused `BuildUrlWithQuery`. (T32)
- The library target now compiles with `/W4 /permissive-` on MSVC and `-Wall -Wextra -Wpedantic -Wconversion -Wshadow` on GCC/Clang, and warnings are still errors in the presets. `/bigobj` is no longer needed now that the client headers are slimmer (T20). (T32)
- `BlockBlobClient`, `PageBlobClient` and `AppendBlobClient` are now `final` and derive publicly from `BlobClient`; each async overload is a one-line call to `Private::InitiateClientOperation`, shrinking the leaf headers from 888/785/420 to 241/172/100 lines. The per-file `LegacyAdapter`/`MakeError` copies are replaced by a single `Private::MakeError` and internal helpers complete with `std::expected` directly. (T20)
- `OpenSSL::Crypto`, `Boost::algorithm`, `Boost::property_tree`, `Boost::url` and `Boost::uuid` are now PRIVATE link dependencies. Only `aveva::http-client` and `Boost::asio` stay PUBLIC, and the installed config `find_dependency`s every Boost component the static library needs. (T32)
- `examples/` builds either in-tree or standalone against an installed package (`find_package(aveva-azure-client)`), and a new `installed_package_consumer` CI job checks that path. `quick_start` is now a runnable coroutine sample driven by `AZURE_BLOB_ENDPOINT`/`AZURE_BLOB_CONTAINER`/`AZURE_BLOB_SAS`; it exits 0 when they are unset. (T32)
- The clang-tidy CI step now fails the build. `.clang-tidy` turns `clang-diagnostic-*`, `bugprone-use-after-move`, `bugprone-dangling-handle` and `concurrency-*` into errors (all currently clean) and lints `examples/` too. (T32)
- XML responses are parsed with rapidxml directly instead of being turned into a `boost::property_tree` first. It is the same parser `read_xml` uses, with the same flags and the same text, CDATA, entity and namespace handling, but no `std::string` key and value is allocated per node. `BM_ParseListBlobsResultXml5000` (a full 5,000-blob page) went from 262 ms to 28 ms in Release, and the small-page benchmark went from 29.5 µs to 3.4 µs. (T17)
- `CachingTokenCredential` no longer makes callers wait for a refresh while the cached token is still valid: inside the refresh window it completes immediately with the cached token and starts (or joins) a single background refresh. Only callers with no token, or an expired one, wait. A failed background refresh no longer surfaces an error to callers holding a valid token; it starts the existing 30 s retry backoff instead. (T18)
- The coverage gate now measures library code only: `tests/` is no longer in the gcovr filter, and throw/unreachable branches are excluded. CONTRIBUTING.md documents the matching local gcovr and OpenCppCoverage commands. (T25)
- Shared Key requests are signed by one `SharedKeySigner` per connection that decodes the account key and keys HMAC-SHA256 once; each signature duplicates the keyed context instead of decoding the key and calling `EVP_MAC_fetch` on every request. There is now a single string-to-sign implementation. (T12)
- Client options are normalised and validated once into an immutable connection state; `BlobServiceClient::GetBlobContainerClient` and `BlobContainerClient::Get*BlobClient` share it (and the signer) instead of copying and re-validating the options for every child client. Child clients now validate the container/blob name and throw `std::invalid_argument` for invalid names. (T14)
- A successful response that cannot be parsed now fails with `BlobStorageErrorCode::InvalidResponse` (previously `std::errc::invalid_argument`, indistinguishable from caller validation errors), with the parser's diagnostic in `BlobStorageError::Message` and the response's status and request id. Download responses with an unexpected `Content-Range` or length report the same code. (T21)
- A `304 Not Modified` response is reported as `ConditionNotMet` instead of a generic `ServiceError`. (T21)
- Downloads (R04/T04/T11): `DownloadAsync` and every `DownloadToAsync` overload of all three blob clients now
  run on one download engine (`src/BlobDownload.cpp`) that owns all of its state. The first request is a ranged
  GET of `ChunkSize` bytes (default 4 MiB) that learns the blob size and ETag; a 0-byte blob's 416 falls back to
  one un-ranged GET. The rest is fetched in `ChunkSize` ranges with up to `Concurrency` requests in flight and
  written in order through a reorder window of at most `Concurrency` chunks, so destinations are never seeked
  and peak buffered memory is about `Concurrency * ChunkSize`. Later chunks send `If-Match: <first ETag>`
  (unless the caller set `If-Match`), so an overwrite during the download fails with `ConditionNotMet` instead
  of producing a torn result. Each request's response-body limit is raised to at least the range length plus
  64 KiB, so blobs larger than the transport's 8 MiB default now download with default options (including
  `DownloadAsync`, which assembles the chunks in memory). An explicit `Range` is downloaded in chunks too.
  On failure or cancellation the in-flight chunk requests are cancelled and drained before the single
  completion. Results: `Properties.ContentLength` and `BytesWritten` are the number of bytes downloaded, and
  `ContentRange` is the downloaded range when the service answered with 206
- `DownloadToAsync` now writes at the stream's current position instead of seeking to offset 0 first
- File downloads (all clients, all overloads) go through the same temporary-file-and-rename path; the
  `BlockBlobClient` path overloads no longer truncate the target before the download. (T06)
- `UploadFromAsync` (R03/T10): the stream and file overloads now share one upload engine that owns all of its
  state. Up to `Concurrency` Put Block requests are in flight at once, each reading directly into one of at most
  `Concurrency` reusable `BlockSize` buffers and sent zero-copy. Blocks are committed in order whatever the
  completion order. Each stage has its own cancellation signal; the first failure (or parent cancellation)
  cancels the outstanding stages, and the operation completes exactly once after they have drained. File
  uploads that would need more than 50,000 blocks grow `BlockSize` automatically (up to 4000 MiB); stream
  uploads fail with `invalid_argument` instead
- Put Block requests (`StageBlockAsync` and `UploadFromAsync` staging) now send only `x-ms-lease-id` from the
  request conditions; `If-Match`/`If-None-Match`/date conditions apply only to the final Put Block List or
  Put Blob. (T08)
- Retry policy (T15/R05): every request now goes through a single non-template retry pipeline
  (`src/RequestPipeline.cpp`) that owns all of its state. It retries HTTP 408/429/500/502/503/504 and
  transient transport errors (resolve/connect/read/write failures, timeouts, connection reset/aborted/refused)
  with exponential backoff and jitter capped at `RetryOptions::MaxDelay`, and honours
  `x-ms-retry-after-ms` / `Retry-After`. Each attempt gets a fresh `x-ms-date` and is re-signed (SharedKey,
  using the cached signer when connection state provides one) or re-authorized (token credential), while keeping
  the same `x-ms-client-request-id`. Retries send a non-owning view of the original body instead of copying it
- `DownloadAsync` (`BlockBlobClient`, `PageBlobClient`, `AppendBlobClient`) no longer keeps a second
  full copy of the downloaded blob alive for the lifetime of the returned `Response<T>`: the raw
  HTTP response body is now moved into `Models::DownloadBlobResult::Content` instead of copied,
  freeing the response's own copy once parsing completes (`DownloadToAsync`, which never
  materializes `Content`, was already fixed this way)
- `DownloadToAsync` (all blob client classes) no longer materializes the downloaded blob twice: the
  response body is now streamed directly from the raw HTTP response into the destination
  `std::ostream`/file instead of first being copied into `Models::DownloadBlobResult::Content` and
  then copied again into the caller's stream. Peak memory for `DownloadToAsync` is now ~1x the blob
  size instead of ~2-3x. `DownloadAsync` (which returns `Content` to the caller) is unaffected.
- vcpkg manifest now uses feature-gated test and benchmark dependencies, while the registry baseline remains pinned in `vcpkg-configuration.json`
- Installed CMake package config now resolves `aveva-http-client` and OpenSSL dependencies explicitly
- Packaging/install guidance now documents exact-version package matching from the shared `aveva_install_library` helper
- Every `...Async` completion token's associated executor, allocator, and cancellation slot are now
  genuinely honored: completions are dispatched onto the token's associated executor (falling back to
  the client's own executor when the token has none) instead of the association being accepted but
  never actually used to redirect execution
- `StaticTokenCredential` and `CachingTokenCredential` gained an optional trailing
  `boost::asio::any_io_executor` constructor parameter; when supplied, `GetTokenAsync` posts its
  completion onto it instead of invoking synchronously (default remains unset/synchronous for
  source compatibility)
- `aveva-http-client` bump: `HttpRequest::SetBodyView(std::span<const std::byte>)` (plus
  `HasBodyView()`/`GetBodyView()`/`GetBodySize()`), a non-owning alternative to `SetBody()` for
  callers that can guarantee the referenced buffer outlives the request's completion handler.
  `BlockBlobClient::UploadFromAsync`'s internal block-staging path (both the `std::filesystem::path`
  and `std::istream&` overloads) now uses this to stage each block directly from its
  already-owned `shared_ptr<std::vector<std::byte>>` buffer instead of copying it into the request
  body, reducing peak allocations for block uploads to roughly 1x the payload size (previously
  ~2-3x via `SetBody(Private::BytesToString(content))`). The public `span`-based `UploadAsync`/
  `StageBlockAsync`/`UploadPagesAsync`/`AppendBlockAsync` overloads are unaffected and continue to
  copy via `SetBody`, to avoid pushing `SetBodyView`'s extended lifetime contract onto callers;
  the `std::string`-based overloads of the same methods now move directly into `SetBody` instead
  of first converting to a span, eliminating one redundant copy there as well
- Every `...Async` overload across all five blob clients (`BlockBlobClient`, `PageBlobClient`,
  `AppendBlobClient`, `BlobContainerClient`, `BlobServiceClient`) now defaults its
  `CompletionToken` parameter to `boost::asio::default_completion_token<executor_type>::type`
  (`boost::asio::deferred_t`, since `executor_type` does not specialize
  `default_completion_token_type`). Omitting the token entirely, e.g.
  `co_await client.UploadAsync(bytes);`, now yields a deferred, directly `co_await`-able operation
  with no explicit `boost::asio::use_awaitable` required. Overloads that are otherwise ambiguous
  with a sibling overload taking an options struct (e.g. `UploadAsync(content, options)` vs.
  `UploadAsync(content, token)`) constrain the defaulted token with a `requires` clause excluding
  that options type, so no overload-resolution ambiguity was introduced. Callers that continue to
  pass an explicit token/callback see no behavioral change
- Every `...Async` call across all five blob clients now forwards its `CompletionToken` as an
  rvalue (via a new internal `Private::InitiateAsync` wrapper around `boost::asio::async_initiate`)
  instead of passing it unforwarded, letting move-only completion tokens (e.g. a lambda capturing a
  `std::unique_ptr`) propagate without an unwanted copy. The fix required routing every call site
  through `Private::InitiateAsync`, which specifies only the completion `Signature` (never
  `CompletionToken`) to `boost::asio::async_initiate`; specifying `CompletionToken` explicitly binds
  to a non-const-lvalue-only overload of `async_initiate` that cannot accept a forwarded rvalue for
  any token kind, plain callback or genuine `async_result`-specialized token alike, which is why a
  naive `std::forward<CompletionToken>(token)` at the old call sites failed to compile
- `Private::BindAssociationsAndAdoptCancellation` now skips the `boost::asio::bind_allocator` wrap
  entirely when a completion handler has no explicit associated allocator (the common case: a plain
  lambda/`std::function`-style callback, which is associated with `std::allocator<void>`) and calls
  `boost::asio::dispatch` directly instead. The `bind_allocator`-wrapped path still runs for
  handlers that genuinely associate a different allocator

### Fixed
- `RedactHeaderForDiagnostics` now redacts the SAS `sig` in `x-ms-copy-source` and any header value that is an http(s) URL. (B16)
- Renew/Change/Release lease (blob and container) now reject an empty `LeaseId` (and empty `ProposedLeaseId` for Change) with `invalid_argument` instead of sending a malformed request. (B11)
- A server `Retry-After` / `x-ms-retry-after-ms` hint is now honoured (up to 24 h) instead of being clamped to `RetryOptions::MaxDelay`; `MaxDelay` caps only the computed backoff. (B10)
- SharedKey signing now combines repeated `x-ms-*` headers and repeated query parameters into a single `name:v1,v2` canonical entry, as the service expects. (B09)
- Removed stray control characters from a header comment and made the shared base64 validator's error messages generic instead of naming `SharedKey.AccountKey`. (B07)
- Token responses carrying only `expires_on` are accepted only when the remaining lifetime is between 1 second and 24 hours (like `expires_in`); out-of-range values, previously able to overflow `system_clock`, are an `InvalidResponse`. (B06)
- `DownloadToAsync`: a `200` reply to the ranged probe (a server/proxy that ignored `Range`) with `Range.Offset > 0` now fails with `InvalidResponse` instead of silently returning bytes from the wrong offset; with `Offset == 0` and `Length` set the body is truncated to the requested length. (B05)
- Any request header (or credential token request header) whose name or value contains CR, LF or NUL now fails with `std::errc::invalid_argument` without being sent, closing a header-injection vector through metadata values, `BlobHttpHeaders`, lease IDs, conditions and `ManagedIdentityCredentialOptions::IdentityHeader`. (B03)
- `UploadFromAsync` block IDs now carry a random per-upload prefix, so concurrent uploads (or a retry overlapping an abandoned upload) to the same blob can no longer overwrite each other's uncommitted blocks and commit a mix of sources. `Models::EncodeBlockId` is unchanged. (B01)
- `UploadFromAsync` no longer throws out of the async API or `io_context::run()` (e.g. `std::bad_alloc` for a huge `BlockSize`): exceptions are mapped to errors. `UploadFromOptions::BlockSize` above 4000 MiB fails with `invalid_argument` before any I/O, and a malformed commit response now fails with `BlobStorageErrorCode::InvalidResponse`. (B04)
- `ParseBreakBlobLeaseResult` assigned `*parsed` (an `int`) into the `std::optional<int> LeaseTimeSeconds` field instead of assigning `parsed` directly, an optional-dereference round trip flagged by clang-tidy 19's `bugprone-optional-value-conversion` (found via the new `LinuxDebugLlvm` preset).
- Cancelling an operation while its token credential is still fetching a token now completes the operation promptly with `operation_canceled` instead of waiting for the credential; the late token is ignored and no request is sent.
- `Content-Range` headers whose total size is not greater than the range end (for example `bytes 0-9/5`) are now rejected as invalid instead of being accepted.
- `DeleteIfExists`/`CreateIfNotExists` on blob, block blob, append blob and container clients now return the real HTTP response (status, headers, request id) when the expected 404/409 is suppressed, instead of an empty synthetic response (T24).
- List Blobs now decodes `<Name Encoded="true">` percent-encoded blob and prefix names (T23).
- Unit tests no longer hard-code `D:\real_work\...` paths; they now pass on Linux and other machines.
- `ParseRetryAfter` did not compile with GCC/libstdc++ (`std::min` of `unsigned long` and `unsigned long long`).
- `ListBlobContainers` now parses each container's lease status/state/duration, public access, immutability/legal-hold flags and encryption-scope settings. They were already modelled by `BlobContainerProperties` but were left at their defaults. (T22)
- The Shared Key string-to-sign hard-coded the Content-Encoding and Content-Language slots to empty. It now uses the request headers, so a request that carries them is signed correctly. (T22)
- `BlobServiceClientOptions`/`BlobContainerClientOptions`/`BlobClientOptions::DefaultRequestOptions` was stored but never used. Operations now use it (and child clients inherit it) when no per-call options are given. (T19)
- Shared Key signing of a PATCH/OPTIONS/TRACE/CONNECT request reached `std::unreachable()` (undefined behaviour) because the method-to-string switch only covered five methods. (T32, found by clang-tidy)
- `cmake --install <build> --prefix <dir>` installed the CMake config package to the configure-time absolute `CMAKE_INSTALL_PREFIX` (e.g. Program Files) instead of `<dir>/share/aveva-azure-client`. (T32)
- `.clang-tidy` used the invalid value `PascalCase` (it should be `CamelCase`) for the identifier-naming options, so those options were ignored. (T32)
- `ListBlobs` now returns each blob's metadata. Azure sends `<Metadata>` as a sibling of `<Properties>`, but the parser only looked for it inside `<Properties>`, so `BlobItem::Properties.Metadata` was always empty. (T17)
- The `CILinuxDebugASan` and `CILinuxDebugUBSan` presets inherited `_CIBase` first, so its `ENABLE_ASAN=OFF`/`ENABLE_UBSAN=OFF` won and the CI sanitizer jobs ran uninstrumented. Each now re-enables its sanitizer explicitly. (T30)
- The upload throughput benchmarks now drive the client's executor, so their (posted) completions actually run, and `SharedKeyBenchmarks.cpp` (`BM_SharedKeySign`, `BM_SharedKeySignOneShot`, `BM_GetBlockBlobClient`) is built. (T12, T14)
- Operations no longer invoke their completion handler before the initiating `...Async` call returns when the
  `IHttpClient` completes inside `SendAsync`; the retry pipeline now posts such completions. A new guard test
  also covers early local failures (directory/missing-file paths, zero-length ranges, invalid block size). (T26)
- `DownloadToAsync` kept references to locals and by-value parameters of the initiating call (use-after-return
  once completions are truly asynchronous), moved a `std::move_only_function` while it was running, and on a
  failure completed while other chunk requests were still writing to the caller's stream or the already-removed
  temporary file. (R04)
- `BlockBlobClient::DownloadToAsync(path)` invoked the completion inline when the target could not be opened
- `UploadFromAsync` could commit twice, complete before in-flight stage requests had finished (while they still
  referenced the caller's stream), and leaked its state through a self-referencing `std::function`. (R03)
- Cancellation through a completion token's associated cancellation slot (e.g.
  `boost::asio::bind_cancellation_slot`) never worked on MSVC: every public `...Async` initiation passed
  `BindAssociationsAndAdoptCancellation(handler, ..., requestOptions)` and `std::move(requestOptions)` as
  arguments to the same call, and with right-to-left argument evaluation the options were moved before the
  slot was adopted. The handler is now bound in a separate statement first (R05)
- Retry pipeline (R05): removed the self-referencing `shared_ptr<std::function>` that leaked every request,
  the by-reference captures of caller options, the ignored timer error, and the unseeded, non-thread-safe
  `std::rand()` jitter. Cancelling during backoff now cancels the timer and completes with
  `std::errc::operation_canceled` without sending more requests. Cancelling an in-flight attempt cancels that
  attempt and is not retried. SharedKey signing failures are posted instead of completing inline
- Fix declare-before-use build break in `BlockBlobClient::UploadFromAsyncImpl(path)` (it called
  `UploadFromAsyncIstreamConcurrent` before that function was declared) and restore correct
  indentation/formatting that had been mangled onto fewer lines (R01)
- Fix a double-commit race in `UploadFromAsyncIstreamConcurrent`: both `State::CommitIfDone()` and
  the stage-completion callback could independently observe all stages complete and each call
  `CommitBlockListAsync`, issuing 5 HTTP requests instead of 4. Guarded with a new atomic
  `CommitStarted` compare-exchange so only one path commits (R01)
- Remove `DEBUG-`/`fprintf(stderr, ...)` instrumentation from production code paths, including one
  in `ComputeSharedKeyAuthorization` that logged the full computed `Authorization` header (a
  credential-leak risk) and an unused `std::regex` validation built on every signed request; add a
  CI grep gate (`no_debug_output` lint job) that fails the build if `DEBUG-`/`fprintf(stderr` ever
  reappears under `src/`/`include/` (R02)
- `AppendBlobClient`'s `LegacyAdapter` no longer silently swallows exceptions thrown by user
  completion handlers in a `try/catch` that only logged and discarded them; exceptions now
  propagate like the other clients' `LegacyAdapter` copies (R02)
- Register `tests/T06_DownloadToFileTests.cpp` in `tests/CMakeLists.txt` (it was never compiled, so
  none of its tests ran) and fix two incorrect test expectations in it (byte-count mismatch;
  assuming a deferred-only `FakeHttpClient` completion shape) (R06)
- `tests/T10_ConcurrentUploadTests.cpp`'s cancellation test previously passed only because it
  manually failed every pending request itself, never exercising real cancellation. `FakeHttpClient`
  now honors `HttpRequestOptions`' cancellation slot (failing the in-flight request with
  `operation_canceled` when the signal emits), and the test now asserts cancellation alone
  completes the operation; restored the upload body-order assertions that had been dropped from
  `UploadFromAsync_Concurrent_IssuesAndCompletesInFlight` (R06)
- Move `TestHooks.hpp` (upload-buffer allocation/peak-bytes counters used only by unit tests) out of
  the public, installed `include/` tree into the private `src/` tree (R06)
- Remove a stray `#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>` that lived inside
  `namespace AVEVA::AzureClient::Private` in `src/BlobRequestHelpers.hpp`, and a dead code block in
  `NormalizeConnectionOptions` (`src/BlobRequestHelpers.cpp`) that built a `ConnectionState` and
  discarded it (R06)
- Remove unused `UploadFromOptions::SingleUploadThreshold` (declared but never read by any upload
  path); it will be reintroduced when actually implemented as part of T10 (R06)
- Stop committing generated test-result files (`fail.xml`, `single.xml`, `single-test.xml`,
  `tests-report.xml`, `tests-results.xml`, `Testing/Temporary/*`); add `*.xml` and `Testing/` to
  `.gitignore` (R06)
- Fix build error converting boost::urls::pct_string_view to std::string on MSVC by constructing std::string from the view.
- Preserve existing service endpoint path when building blob/container URLs and avoid producing duplicate slashes when appending container/blob segments; add test coverage for Azurite-style path endpoints (T07).
- Ensure SharedKey-signed stage-block requests for zero-copy uploads use the request body size via HttpRequest::GetBodySize so `SetBodyView` uploads are signed correctly; add an integration unit test covering UploadFromAsync with SharedKey (T08).
- Tests now declare their direct `Boost` and `OpenSSL` package requirements instead of relying on transitive discovery
- ASan presets now point at a real overlay triplet so Windows sanitizer builds can configure
- Early-exit validation failures (invalid block id, invalid/overflowing page range, unopenable
  upload/download file, invalid download range, etc.) no longer invoke their completion handler
  synchronously from within the initiating `...Async` call, per Boost.Asio's completion-handler
  contract; the completion is now posted and runs once the executor is driven
- `SendAuthorizedRequestAsync` (the internal helper every blob client authorization path routes
  through) now detects when an `ITokenCredential::GetTokenAsync` implementation completes
  synchronously (same-stack, before its own initiating call returns) and defers the rest of the
  request via `boost::asio::post` in that case. Previously, the built-in
  `StaticTokenCredential`/`CachingTokenCredential` (and any user-supplied `ITokenCredential` that
  simply invokes its handler inline) would let the entire authorized-request chain -- header
  construction and `IHttpClient::SendAsync` -- run before the client's own `...Async()` call
  returned to its caller, violating Boost.Asio's "never complete before the initiating function
  returns" contract. This is fixed universally at the call site rather than requiring every
  `ITokenCredential` implementation to opt in via its own executor parameter (which remains
  available on the built-in credential types for standalone/external use, but is no longer required
  for correctness when used through this library)
- `tests/FakeHttpClient` now honors the `IHttpClient` async contract by default: non-deferred
  (`CompleteInline == false`, the new default) completions are posted onto the fake client's
  `io_context` instead of being invoked inline from within `SendAsyncErased`, so tests now exercise
  the real "completion never runs before the initiating call returns" code path instead of an
  impossible re-entrant one. All ~160 affected tests across `AppendBlobClientTests.cpp`,
  `AsyncBehaviorTests.cpp`, `BlobContainerClientTests.cpp`, `BlobFeatureSmokeTests.cpp`,
  `BlobServiceClientTests.cpp`, `BlockBlobClientTests.cpp`, `PageBlobClientTests.cpp`,
  `T04_ConcurrencyTests.cpp`, and `T11_ParallelDownloadAcceptanceTests.cpp` were updated to call
  `httpClient.Poll()` (draining the fake client's `io_context`) before asserting on a completion.
  Added a new table-driven guard test,
  `AsyncBehaviorTests.NoPublicOperationCompletesBeforeTheInitiatingCallReturns`, asserting this
  invariant across every public operation on all five blob client types (T26)
- Flipping `FakeHttpClient`'s completion default to async exposed a genuine, previously-masked
  crash (access violation) in `SendAuthorizedRequestAsync`'s retry loop
  (`src/BlobRequestHelpers.hpp`): `attemptSend`, `schedule_retry`, and `do_send` used blanket `[&,
  ...]` captures that implicitly captured true stack locals (`maxAttempts`, `baseDelay`,
  `is_transient_failure`, and `attemptSend`'s own local `finish`) by reference, even though
  `attemptSend` is stored in a `shared_ptr` and re-invoked later (via a `steady_timer`, after
  `SendAuthorizedRequestAsync` has already returned) -- a dangling-reference use-after-return.
  Changed all three lambdas to fully explicit capture lists: the true locals above are now captured
  by value, while `httpClient`/`options` (reference parameters) remain captured by reference, since
  that binds directly to the caller-owned referent rather than to this function's stack frame. This
  is a minimal, scoped lifetime fix only; the full `RetryState`-owning rewrite tracked by R05 (the
  self-capturing-`shared_ptr` leak, missing cancellation-slot hookup, and retry-policy gaps) remains
  open (R05/T26)
- `T04_ConcurrencyTests.SeekableStream_IssuesConcurrentRangeRequests` and
  `T11_ParallelDownloadAcceptanceTests.ConcurrentChunksCompleteOutOfOrder` completed their deferred
  chunk responses before the probe request's now-posted completion, so
  `DownloadBlobToAsync_Concurrent`'s `stream.seekp()` to an offset past the (still-empty)
  `std::stringstream`'s end silently failed. Reordered each test to `Poll()` the probe's posted
  completion before completing the deferred chunk responses, matching the write order the tests
  already assumed (T26)

### Breaking
- `TokenCredential`/`BearerToken` with an `http://` `ServiceEndpoint` now throws `std::invalid_argument("Token credentials require an https ServiceEndpoint.")` (bearer tokens would be sent in clear text). Loopback hosts (`localhost`, `127.0.0.1`, `[::1]`) are exempt for Azurite and local proxies; SharedKey and SAS are unaffected. (B02)
- `BlobStorageError::TransportError` is renamed to `BlobStorageError::Code`; it has always carried service and validation codes as well as transport errors. Replace `.TransportError` with `.Code` (T24).
- `BlobStorageErrorCode::InvalidMetadata` is inserted before `InvalidResponse`, so the numeric value of `InvalidResponse` changes. Compare against enumerators, not integers (T24).
- `BreakLeaseOptions::LeaseId` removed: Break Lease no longer sends `x-ms-lease-id`. `Conditions.LeaseId` is ignored by lease operations (the options' own `LeaseId` is used) (T23/T24).
- `AcquireLeaseOptions::DurationSeconds` (`int`, -1 = infinite) is replaced by `std::optional<std::chrono::seconds> Duration` (nullopt = infinite). Durations outside 15–60 s now fail with `invalid_argument` before any request is sent. Likewise `BreakLeaseOptions::BreakPeriodSeconds` becomes `std::optional<std::chrono::seconds> BreakPeriod`, validated to 0–60 s. (T22)
- `AccessTier` options members, `DeleteBlobOptions::DeleteSnapshotsOption` and `StartBlobCopyFromUriResult::CopyStatus` are now the typed `Models::AccessTier`/`DeleteSnapshotsOption`/`CopyStatus` instead of `std::string`. Assigning or comparing with strings still works; use `.ToString()` where a `std::string` is needed. `LeaseStatus`/`LeaseState`/`LeaseDurationType` moved from `BlobContainerModels.hpp` to `BlobModels.hpp` (still included by the former). (T22)
- Removed the internal `PrecomputedBasePrefix`, `PrecomputedBasePath` and `Connection` members from `BlobClientOptions`, `BlobContainerClientOptions` and `BlobServiceClientOptions`, and made the `BlobContainerClient(IHttpClient&, std::shared_ptr<const Private::ConnectionState>, std::string)` constructor private. (T14)
- Calls that pass `{}` as the options argument of `DownloadToAsync` are now ambiguous, because the new
  `DownloadToOptions` overloads also accept it; name `DownloadBlobOptions` or `DownloadToOptions` explicitly
- Prior refactors in this development cycle converted client/options configuration to aggregate-style option structs and made client storage/reference semantics assignable while preserving the callback-only async model
- `aveva-http-client`: `IHttpClient` implementers must rename their `SendAsync` override to
  `SendAsyncErased` and implement the new pure-virtual `get_executor()`

## [0.0.1] - 2026-09-28

### Added
- Initial package metadata, exported CMake target (`aveva::azure-client`), overlay port, blob/container/service clients, auth helpers, typed error handling, and comprehensive test coverage
