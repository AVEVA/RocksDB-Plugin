#pragma once

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <optional>
#include <stdexcept>
#include <utility>

namespace AVEVA::AzureClient
{
    // Pairs a parsed, strongly typed result model with the raw HTTP response it was derived
    // from, mirroring the Response<T> pattern used by the Azure SDK for C++.
    template <class T> class Response
    {
      public:
        Response() = default;

        Response(T value, HttpResponse rawResponse, std::optional<BlobStorageError> error = std::nullopt)
            : m_value(std::move(value)), m_rawResponse(std::move(rawResponse)), m_error(std::move(error))
        {
        }

        [[nodiscard]] const T& Value() const& noexcept
        {
            return m_value;
        }

        [[nodiscard]] T& Value() & noexcept
        {
            return m_value;
        }

        [[nodiscard]] T&& Value() && noexcept
        {
            return std::move(m_value);
        }

        [[nodiscard]] const HttpResponse& RawResponse() const& noexcept
        {
            return m_rawResponse;
        }

        [[nodiscard]] HttpResponse& RawResponse() & noexcept
        {
            return m_rawResponse;
        }

        [[nodiscard]] HttpResponse&& RawResponse() && noexcept
        {
            return std::move(m_rawResponse);
        }

        // Empty for ordinary successes. Engaged only when the operation deliberately treated a service
        // error as success (e.g. DeleteIfExists on a missing blob, CreateIfNotExists on an existing one):
        // it then describes the suppressed error, RawResponse() is the real error response (404/409/...)
        // and Value() is default-constructed. Genuine failures never reach a Response; they are
        // reported through the operation's error channel instead.
        [[nodiscard]] const std::optional<BlobStorageError>& Error() const& noexcept
        {
            return m_error;
        }

        [[nodiscard]] std::optional<BlobStorageError>& Error() & noexcept
        {
            return m_error;
        }

        [[nodiscard]] std::optional<BlobStorageError>&& Error() && noexcept
        {
            return std::move(m_error);
        }

      private:
        T m_value{};
        HttpResponse m_rawResponse;
        std::optional<BlobStorageError> m_error;
    };

} // namespace AVEVA::AzureClient
