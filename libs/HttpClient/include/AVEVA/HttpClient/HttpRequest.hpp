#pragma once

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace AVEVA
{
    class HttpRequest
    {
      public:
        HttpRequest();

        HttpMethod GetMethod() const noexcept;
        const std::string& GetUrl() const noexcept;
        const std::vector<HttpHeader>& GetHeaders() const noexcept;
        std::vector<HttpHeader>& GetHeaders() noexcept;

        /// Returns the owned body set via SetBody(). Empty if a non-owning body was set via
        /// SetBodyView() -- use GetBodyView() (and HasBodyView()) in that case.
        const std::string& GetBody() const noexcept;

        void SetMethod(HttpMethod method) noexcept;
        void SetUrl(std::string url);
        void SetHeaders(std::vector<HttpHeader> headers);
        void AddHeader(HttpHeader header);
        void ReserveHeaders(std::size_t count);

        /// Sets an owned request body. The bytes are copied synchronously; the caller's buffer
        /// need not outlive this call.
        void SetBody(std::string body);

        /// Sets a non-owning request body view over caller-owned memory.
        ///
        /// LIFETIME CONTRACT: unlike SetBody(), this does not copy `content`. The memory it
        /// points to MUST remain valid and unmodified from this call until the completion
        /// handler for the HttpClient::SendAsync() (or equivalent) operation that consumes this
        /// request has run -- NOT merely until the initiating call returns. Destroying or
        /// mutating the referenced buffer before completion is undefined behavior. Prefer
        /// SetBody() unless you can guarantee this extended lifetime (e.g. the buffer is owned by
        /// a std::shared_ptr kept alive by the caller for the duration of the operation).
        void SetBodyView(std::span<const std::byte> content) noexcept;

        /// Like SetBodyView(), but the request (and every copy of it, and the HTTP operation that sends it) also
        /// holds `keepAlive`, so the viewed memory stays valid for as long as anything can still read it.
        void SetBodyView(std::span<const std::byte> content, std::shared_ptr<const void> keepAlive) noexcept;

        /// The owner passed to SetBodyView(content, keepAlive); null otherwise.
        [[nodiscard]] const std::shared_ptr<const void>& GetBodyKeepAlive() const noexcept;

        /// Moves the owned body out of the request, leaving it empty. Has no effect on a body view.
        [[nodiscard]] std::string ReleaseBody() noexcept;

        /// True if the active body was set via SetBodyView() rather than SetBody().
        [[nodiscard]] bool HasBodyView() const noexcept;

        /// Returns the non-owning body view set via SetBodyView(). Empty (and meaningless) if
        /// HasBodyView() is false.
        [[nodiscard]] std::span<const std::byte> GetBodyView() const noexcept;

        /// Returns the body length in bytes regardless of which representation (SetBody() or
        /// SetBodyView()) is active.
        [[nodiscard]] std::size_t GetBodySize() const noexcept;

      private:
        HttpMethod m_method = HttpMethod::Get;
        std::string m_url;
        std::vector<HttpHeader> m_headers;
        std::string m_body;
        std::span<const std::byte> m_bodyView;
        std::shared_ptr<const void> m_bodyKeepAlive;
        bool m_hasBodyView = false;
    };
} // namespace AVEVA
