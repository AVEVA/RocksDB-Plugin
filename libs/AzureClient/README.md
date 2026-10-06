# aveva-azure-client

`aveva-azure-client` is a C++23, callback-based Azure Blob Storage client built on top of `aveva-http-client`. It provides focused blob/container/service operations, Azure-style response models, and CMake package exports for internal consumers.

## Features

- Containers: create, create-if-not-exists, flat/hierarchical listing
- Block blobs: create/upload, stage blocks, commit block lists, download/read, delete, head, leases, metadata
- Page blobs: create, upload/clear pages, resize, download/read, delete, head, page ranges, leases, metadata
- Service: container client factory, user delegation keys
- Auth: Shared Key, SAS, bearer token, and refreshable `ITokenCredential`; Microsoft Entra ID credentials
  (`ClientSecretCredential`, `WorkloadIdentityCredential`, `ManagedIdentityCredential`); service/account SAS builders
- Errors: typed `BlobStorageError` details plus raw `Response<T>` access

## API map

Paths are relative to `include/AVEVA/AzureClient/`; `AzureClient.hpp` includes everything.

| Class | Header | Principal operations |
|---|---|---|
| `BlobServiceClient` | [BlobServiceClient.hpp](include/AVEVA/AzureClient/BlobServiceClient.hpp) | Container-client factory, user delegation key |
| `BlobContainerClient` | [BlobContainerClient.hpp](include/AVEVA/AzureClient/BlobContainerClient.hpp) | Create, create-if-not-exists, list blobs, blob-client factories |
| `BlobClient` | [BlobClient.hpp](include/AVEVA/AzureClient/BlobClient.hpp) | Download, delete, properties, leases, metadata |
| `BlockBlobClient` | [BlockBlobClient.hpp](include/AVEVA/AzureClient/BlockBlobClient.hpp) | Upload, stage block, commit block list |
| `PageBlobClient` | [PageBlobClient.hpp](include/AVEVA/AzureClient/PageBlobClient.hpp) | Create, upload/clear pages, resize, page ranges |
| Credentials | [Credentials.hpp](include/AVEVA/AzureClient/Credentials.hpp), [ITokenCredential.hpp](include/AVEVA/AzureClient/ITokenCredential.hpp) | Shared Key, bearer token, Entra ID credentials |
| SAS builders | [Sas.hpp](include/AVEVA/AzureClient/Sas.hpp) | Service and account SAS generation |
| Options | [BlobClientOptions.hpp](include/AVEVA/AzureClient/BlobClientOptions.hpp), [BlobOperationOptions.hpp](include/AVEVA/AzureClient/BlobOperationOptions.hpp), [WithRequestOptions.hpp](include/AVEVA/AzureClient/WithRequestOptions.hpp) | Client construction, per-operation and per-request options |
| Results and errors | [Response.hpp](include/AVEVA/AzureClient/Response.hpp), [BlobStorageError.hpp](include/AVEVA/AzureClient/BlobStorageError.hpp), [BlobStorageErrorCode.hpp](include/AVEVA/AzureClient/BlobStorageErrorCode.hpp) | `Response<T>`, `BlobStorageError`, error codes |
| Models | [BlobModels.hpp](include/AVEVA/AzureClient/Models/BlobModels.hpp), [BlobContainerModels.hpp](include/AVEVA/AzureClient/Models/BlobContainerModels.hpp), [BlobServiceModels.hpp](include/AVEVA/AzureClient/Models/BlobServiceModels.hpp) | Result and property types |

## Quick start

```cpp
#include <AVEVA/AzureClient/AzureClient.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <iostream>
#include <memory>
#include <string>

void PrintError(const AVEVA::AzureClient::BlobStorageError& error)
{
    std::cerr << "Error: " << error.Message << " (code=" << error.Code.message() << ", http=" << error.StatusCode
              << ", service code=" << error.ErrorCode << ", request id=" << error.RequestId << ")\n";
    // Compare against portable conditions rather than strings; transport failures have StatusCode 0.
    if (error.Code == std::errc::timed_out)
    {
        std::cerr << "The request timed out; it is safe to retry idempotent operations.\n";
    }
}

void UploadAndDownload(
    AVEVA::IHttpClient& httpClient,
    const std::string& endpoint,
    const std::string& containerName,
    const std::string& sasToken)
{
    using namespace AVEVA::AzureClient;

    BlobContainerClient containerClient{
        httpClient,
        BlobContainerClientOptions{
            .ServiceEndpoint = endpoint,
            .ContainerName = containerName,
            .SasToken = sasToken}};

    auto blobClient = std::make_shared<BlockBlobClient>(containerClient.GetBlockBlobClient("hello.txt"));

    blobClient->UploadAsync(
        "hello from aveva-azure-client",
        [blobClient](std::expected<Response<Models::UploadBlockBlobResult>, BlobStorageError> uploadResult)
        {
            if (!uploadResult)
            {
                std::cerr << "Upload failed: ";
                PrintError(uploadResult.error());
                return;
            }

            blobClient->DownloadAsync(
                [](std::expected<Response<Models::DownloadBlobResult>, BlobStorageError> downloadResult)
                {
                    if (!downloadResult)
                    {
                        std::cerr << "Download failed: ";
                        PrintError(downloadResult.error());
                        return;
                    }

                    const auto& content = downloadResult->Value().Content;
                    const std::string text(reinterpret_cast<const char*>(content.data()), content.size());
                    std::cout << text << '\n';
                });
        });
}
```

## Callback-based API model

Every operation is a Boost.Asio `async_initiate`-based `...Async` call that accepts any Boost.Asio
`CompletionToken`. The default remains a plain callback receiving a single
`std::expected<Response<T>, BlobStorageError>`:

```cpp
client.GetPropertiesAsync(
    [](std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::BlobProperties>, AVEVA::AzureClient::BlobStorageError> result)
    {
        if (!result)
        {
            return;
        }

        std::cout << result->Value().ETag << '\n';
    });
```

### Completion tokens: futures and coroutines

Beyond plain callbacks, every `...Async` overload also accepts `boost::asio::use_future` for a
`std::future`, or is omitted entirely to get a deferred, directly `co_await`-able operation (the
token parameter defaults to this client's `default_completion_token`, i.e. `boost::asio::deferred_t`):

```cpp
boost::asio::awaitable<void> PrintBlobETag(AVEVA::AzureClient::BlockBlobClient& client)
{
    // No explicit completion token needed: omitting it defaults to a deferred, co_await-able operation.
    auto result = co_await client.GetPropertiesAsync();
    if (result)
    {
        std::cout << result->Value().ETag << '\n';
    }
}
```

### Request options: timeouts, body limits, and cancellation

`AVEVA::HttpRequestOptions` holds the per-request timeout (default 30 s), the response body limit
(default 8 MiB), and an optional `boost::asio::cancellation_slot`. Set client-wide values with
`DefaultRequestOptions` on `BlobServiceClientOptions` / `BlobContainerClientOptions` / `BlobClientOptions`.
Container and blob clients obtained from a parent client inherit its defaults. To override them for
one call, wrap the completion token with `WithRequestOptions`, which keeps the token in last position:

```cpp
AVEVA::HttpRequestOptions slow;
slow.SetTimeout(std::chrono::minutes{5});

auto deleted = co_await client.DeleteAsync(AVEVA::AzureClient::WithRequestOptions(slow));                 // deferred
auto future = client.GetPropertiesAsync(AVEVA::AzureClient::WithRequestOptions(slow, boost::asio::use_future));
```

The older trailing `requestOptions` argument (`Op(args..., token, requestOptions)`) still works.
When several sources are given, `WithRequestOptions` wins over the trailing argument, and the
trailing argument wins over the client default. A cancellation slot associated with the completion
token (for example via `boost::asio::bind_cancellation_slot` or `cancel_after`) is used only if the
effective request options carry none.

### Defaults and limits

| Setting | Default | Where |
| --- | --- | --- |
| Per-request timeout | 30 s | `HttpRequestOptions::SetTimeout`, `DefaultRequestOptions` |
| Response body limit | 8 MiB | `HttpRequestOptions`; downloads raise it per request to the chunk size + 64 KiB, so large blobs download with default options |
| Download chunk size / concurrency | 4 MiB / 1 | `DownloadToOptions::ChunkSize` / `Concurrency` |
| Retries | 3 retries, 800 ms initial delay, 60 s max delay | `RetryOptions` (`Retry` member of the client options) |

### Parallel transfers

`DownloadToAsync(..., DownloadToOptions{...})` first fetches one `ChunkSize` range to learn the blob size and
ETag, then fetches the rest with up to `Concurrency` ranged GETs in flight. Chunks are written in order (the
destination is never seeked), so peak buffered memory is about `Concurrency * ChunkSize`. Later chunks send
`If-Match` with the first ETag, so a blob overwritten mid-download fails with `ConditionNotMet` rather than
producing a torn result. File downloads write to a temporary file and rename it on success.

On failure or cancellation of either, the outstanding requests are cancelled and drained before the single
completion runs.

### Retry policy

Every request goes through one retry pipeline. It retries HTTP 408, 429, 500, 502, 503 and 504 and transient
transport errors (resolve/connect/read/write failures, timeouts, connection reset/aborted/refused) with
exponential backoff and jitter, capped at `RetryOptions::MaxDelay`, and honours `x-ms-retry-after-ms` /
`Retry-After` (up to 24 h, even above `MaxDelay`, which caps only the computed backoff). Each attempt gets a fresh `x-ms-date` and signature but keeps the same
`x-ms-client-request-id`. Set `MaxRetries = 0` to disable retries. `IsTransient(const BlobStorageError&)`
exposes the same classification.

### Endpoints

`ServiceEndpoint` may be a standard account URL (`https://<account>.blob.core.windows.net`) or a path-style
endpoint such as an emulator or proxy (`http://127.0.0.1:10000/devstoreaccount1`). Any path on the endpoint is
preserved, and container and blob segments are appended without duplicate slashes.

## Lifetime, threading, and synchronous throws

- `IHttpClient` is borrowed, not owned. Each client stores a non-owning reference/pointer to the `IHttpClient` passed at construction, so keep the HTTP client alive until the client object and every in-flight `...Async` operation have finished.
- Operations taking `std::span<const std::byte>` (for example `UploadAsync`, `StageBlockAsync`) read the span when the operation is *initiated*, not when the call returns. With a callback or `use_future` that is during the call, but with the default deferred token it is when the returned operation is `co_await`ed or otherwise launched, so the bytes must stay valid until then. Overloads taking `std::string` copy the content and have no such constraint.
- `std::ostream&` passed to `DownloadToAsync(...)` are also borrowed. Keep the stream alive, open, and otherwise stable until the completion handler runs.
- Treat `ITokenCredential` the same way: keep the supplied `std::shared_ptr<ITokenCredential>` and any state it depends on alive until any in-flight token acquisition or request waiting on that token has completed.
- The library does not provide its own executor or thread-affinity guarantee. Completion handlers run on whatever thread the supplied `IHttpClient` / `ITokenCredential` uses, and they may run inline before the initiating `...Async` call returns if the dependency completes synchronously. Write callbacks to tolerate both immediate and deferred invocation.
- Stream and file I/O of `DownloadToAsync(...)` (`ostream::write`, file writes) runs on the threads that complete HTTP requests, i.e. the client's executor. A slow disk or a stream that blocks stalls every other in-flight request on that executor; use a dedicated `io_context` for the HTTP client or a fast stream.
- Cancellation: Asio cancellation slots are not thread-safe. Emit a `cancellation_signal` from the same executor (or strand) the completion handler runs on, for example `boost::asio::post(handlerExecutor, [&signal] { signal.emit(boost::asio::cancellation_type::terminal); })`. Emitting from an arbitrary thread can race with the library releasing the slot when the operation completes.
- Constructor-time option validation still throws synchronously (`std::invalid_argument`) for invalid endpoints, invalid option values, or conflicting credentials. Per-operation validation that used to throw synchronously (for example invalid block IDs, invalid page alignment, or zero-length download ranges) is now reported through the completion handler as `std::errc::invalid_argument`. One exception remains: a Shared Key request can still throw `std::runtime_error` synchronously if OpenSSL cannot initialize or finalize the HMAC operation while signing the request.

## Authorization precedence

If exactly one credential source is configured, requests use this precedence: `SasToken` > `SharedKey` > `TokenCredential` > `BearerToken`.

- `BearerToken` is normalized into an internal `StaticTokenCredential` / `CachingTokenCredential`.
- Supplying more than one credential source is rejected during client construction with `std::invalid_argument`; the library no longer silently picks one and ignores the rest.

### Include policy

`AzureClient.hpp` is a convenience aggregate that pulls in every client, all models, Asio and the credentials. Build-time-sensitive consumers should include only what they use, for example `BlobClient.hpp` or `BlobContainerClient.hpp` plus the specific `Models/` headers.

## Consuming from CMake

```cmake
find_package(aveva-azure-client CONFIG REQUIRED)

target_link_libraries(my-app
    PRIVATE
        aveva::azure-client
)
```

The exported package currently uses exact-version matching via the shared `aveva_install_library` helper.

## Building

This copy is trimmed to what the RocksDB plugin needs (no examples, benchmarks, docs, fuzzing, install test or live integration tests). It is built through the root `CMakeLists.txt` (`-DAVEVA_BUILD_CLIENT_LIBRARY_TESTS=ON` for its unit tests) and the root presets.

## Repository layout

- `include/AVEVA/AzureClient/` public headers
- `src/` library implementation and package config template
- `tests/` unit tests

## License

See [LICENSE](LICENSE). This repository is currently marked for proprietary internal use and may not be redistributed outside AVEVA Group without written authorization. Dependency licenses are listed in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) (draft, pending legal / OSS-compliance confirmation).
