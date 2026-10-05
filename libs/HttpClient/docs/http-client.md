# HTTP client

The public header is `AVEVA/HttpClient/HttpClient.hpp`. Request, response, header,
and error types use the standard library. The public headers include some Boost.Asio
headers (for example `io_context` and the executor types); Beast, Boost.URL, and
OpenSSL remain implementation details.

## Usage

```cpp
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <boost/asio/io_context.hpp>
#include <iostream>
#include <utility>

int main()
{
    boost::asio::io_context runtime;
    auto client = AVEVA::IHttpClient::Create(runtime);

    AVEVA::HttpRequest request;
    request.SetUrl("https://example.com/");
    client->SendAsync(std::move(request),
        [](std::error_code error, AVEVA::HttpResponse response)
        {
            if (error)
            {
                std::cerr << error.message() << '\n';
                return;
            }
            std::cout << response.GetStatus() << '\n' << response.GetBody();
        });

    runtime.run();
}
```

Consumers that create an Asio runtime must link their own `Boost::asio` target
alongside `aveva::http-client`. The library requires C++23.

`IHttpClient::Create` accepts optional `HttpClientOptions`. The default requires
peer and hostname verification, trusts the platform OpenSSL paths, and permits
TLS 1.2 or later. Use `SetTlsVersion` with `TlsVersion::Tls12` or
`TlsVersion::Tls13` to require an exact protocol version. `SetCaFile` and
`SetCaDirectory` select custom trust sources. `SetVerifyPeer(false)` disables
certificate and hostname verification and should only be used for controlled
testing.

## Callback contract

- Requests and callbacks are taken by value. Callbacks may be move-only.
- Completion is dispatched through the supplied runtime, never inline on the
  `SendAsync` call stack. Another runtime thread may execute it before
  `SendAsync` returns. Keep running the runtime until requests finish.
- Each accepted request completes once. Invalid URLs and request data also
  complete asynchronously. An empty callback throws `std::invalid_argument`
  synchronously. Allocation or runtime-resource failures can throw.
- Callback exceptions propagate through `io_context::run()`; callbacks should
  handle their own application errors.
- The client does not run, stop, or restart the runtime. The runtime must outlive
  the client and outstanding work. Request state retains its TLS context, so
  destroying the client does not cancel requests already submitted.
- Each request runs on its own strand (a pooled connection is used by one request at a time). Separate requests can complete
  concurrently when multiple threads run the runtime.
- HTTP statuses, including 4xx and 5xx, are successful transport results. Failures
  return an empty response and a library-owned `std::error_code`. Cancellation reports
  `std::errc::operation_canceled`. Codes are comparable to
  `AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut)` and the other enum values.

## Request handling

URLs must be absolute HTTP or HTTPS URLs, with no embedded credentials. URL
fragments are omitted, encoded paths and queries are preserved, and international
hostnames must be supplied in ASCII/Punycode form. Missing ports use 80 or 443.
Use HTTPS for production traffic.

Headers preserve duplicate entries. The client owns `Host`, `Content-Length`,
`Transfer-Encoding`, and `Connection`; supplied values for these are replaced by
URL-derived authority and fixed-length, keep-alive framing. Bodies can
contain binary bytes, including nulls. Responses are buffered rather than streamed.

`HttpRequest::SetBody(std::string)` copies its argument synchronously; the caller's
buffer need not outlive the call. `HttpRequest::SetBodyView(std::span<const std::byte>)`
is a non-owning alternative that avoids that copy on the wire-serialization path.
**Lifetime contract:** unlike `SetBody`, the memory referenced by `SetBodyView` must
remain valid and unmodified from the call until the request's completion handler has
run -- not merely until `SendAsync` returns. Mutating or destroying the buffer before
completion is undefined behavior. Prefer `SetBody` unless the caller can guarantee
that extended lifetime (for example, a block buffer owned by a `std::shared_ptr` kept
alive by the caller for the duration of the operation).

The positive timeout defaults to 30 seconds and covers resolution, connection,
TLS handshake, writing, and reading once the runtime starts processing the request.
The response body limit defaults to 8 MiB. HEAD responses and informational
responses are handled internally. Completed HTTPS responses are delivered before
a bounded, best-effort TLS shutdown finishes.

Keep-alive connections are pooled per scheme, host, and port, so repeat requests to
the same origin skip resolution, connection, and the TLS handshake.
`SetMaxIdleConnectionsPerHost` caps how many idle connections each origin keeps and
`SetIdleConnectionTimeout` bounds how long one may sit idle; a cap of zero disables
pooling. Reuse prefers the most recently released connection, and an origin over its
cap drops its oldest idle connection.

Idempotent requests are retried once on a new connection when a reused pooled connection turns out to be
stale; other requests are not retried. There is no redirect following, proxy support, content
decompression, protocol upgrade, or CONNECT tunneling.

## TLS trust

HTTPS enables peer and hostname verification, DNS-name SNI, and TLS 1.2 or newer.
Trust roots come from OpenSSL's default verification paths. Configure a CA bundle
using `SSL_CERT_FILE` or a hashed CA directory using `SSL_CERT_DIR` before creating
the client if the OpenSSL installation does not provide usable defaults. This
implementation does not import the Windows certificate store. TLS initialization
failures throw from `Create`; request handshake failures report `TlsFailed`.