// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <compare>
#include <cstdint>

namespace AVEVA {
/// <summary>The family a status code belongs to, which is what most callers actually need to decide on.</summary>
enum class HttpStatusClass : std::uint8_t { NoResponse, Informational, Success, Redirection, ClientError, ServerError };

/// <summary>An HTTP response status code. Construction from and access to the number are explicit.</summary>
class HttpStatusCode {
  public:
    constexpr HttpStatusCode() noexcept = default;
    explicit constexpr HttpStatusCode(unsigned int value) noexcept : m_value(value) {}

    [[nodiscard]] constexpr unsigned int Value() const noexcept { return m_value; }

    /// <summary>The family of this code; <c>NoResponse</c> for 0 and for anything outside 100-599.</summary>
    [[nodiscard]] constexpr HttpStatusClass Class() const noexcept {
        constexpr unsigned int Hundred = 100;
        switch (m_value / Hundred) {
        case 1:
            return HttpStatusClass::Informational;
        case 2:
            return HttpStatusClass::Success;
        case 3:
            return HttpStatusClass::Redirection;
        case 4:
            return HttpStatusClass::ClientError;
        case 5:
            return HttpStatusClass::ServerError;
        default:
            return HttpStatusClass::NoResponse;
        }
    }

    friend constexpr auto operator<=>(HttpStatusCode, HttpStatusCode) noexcept = default;
    friend constexpr bool operator==(HttpStatusCode, HttpStatusCode) noexcept = default;

  private:
    unsigned int m_value = 0;
};

/// <summary>The registered HTTP status codes (RFC 9110 and common extensions).</summary>
namespace HttpStatus {
inline constexpr HttpStatusCode NoResponse{0};
// 1xx
inline constexpr HttpStatusCode Continue{100};
inline constexpr HttpStatusCode SwitchingProtocols{101};
inline constexpr HttpStatusCode Processing{102};
inline constexpr HttpStatusCode EarlyHints{103};
// 2xx
inline constexpr HttpStatusCode Ok{200};
inline constexpr HttpStatusCode Created{201};
inline constexpr HttpStatusCode Accepted{202};
inline constexpr HttpStatusCode NonAuthoritativeInformation{203};
inline constexpr HttpStatusCode NoContent{204};
inline constexpr HttpStatusCode ResetContent{205};
inline constexpr HttpStatusCode PartialContent{206};
inline constexpr HttpStatusCode MultiStatus{207};
inline constexpr HttpStatusCode AlreadyReported{208};
inline constexpr HttpStatusCode ImUsed{226};
// 3xx
inline constexpr HttpStatusCode MultipleChoices{300};
inline constexpr HttpStatusCode MovedPermanently{301};
inline constexpr HttpStatusCode Found{302};
inline constexpr HttpStatusCode SeeOther{303};
inline constexpr HttpStatusCode NotModified{304};
inline constexpr HttpStatusCode UseProxy{305};
inline constexpr HttpStatusCode TemporaryRedirect{307};
inline constexpr HttpStatusCode PermanentRedirect{308};
// 4xx
inline constexpr HttpStatusCode BadRequest{400};
inline constexpr HttpStatusCode Unauthorized{401};
inline constexpr HttpStatusCode PaymentRequired{402};
inline constexpr HttpStatusCode Forbidden{403};
inline constexpr HttpStatusCode NotFound{404};
inline constexpr HttpStatusCode MethodNotAllowed{405};
inline constexpr HttpStatusCode NotAcceptable{406};
inline constexpr HttpStatusCode ProxyAuthenticationRequired{407};
inline constexpr HttpStatusCode RequestTimeout{408};
inline constexpr HttpStatusCode Conflict{409};
inline constexpr HttpStatusCode Gone{410};
inline constexpr HttpStatusCode LengthRequired{411};
inline constexpr HttpStatusCode PreconditionFailed{412};
inline constexpr HttpStatusCode PayloadTooLarge{413};
inline constexpr HttpStatusCode UriTooLong{414};
inline constexpr HttpStatusCode UnsupportedMediaType{415};
inline constexpr HttpStatusCode RangeNotSatisfiable{416};
inline constexpr HttpStatusCode ExpectationFailed{417};
inline constexpr HttpStatusCode MisdirectedRequest{421};
inline constexpr HttpStatusCode UnprocessableContent{422};
inline constexpr HttpStatusCode Locked{423};
inline constexpr HttpStatusCode FailedDependency{424};
inline constexpr HttpStatusCode TooEarly{425};
inline constexpr HttpStatusCode UpgradeRequired{426};
inline constexpr HttpStatusCode PreconditionRequired{428};
inline constexpr HttpStatusCode TooManyRequests{429};
inline constexpr HttpStatusCode RequestHeaderFieldsTooLarge{431};
inline constexpr HttpStatusCode UnavailableForLegalReasons{451};
// 5xx
inline constexpr HttpStatusCode InternalServerError{500};
inline constexpr HttpStatusCode NotImplemented{501};
inline constexpr HttpStatusCode BadGateway{502};
inline constexpr HttpStatusCode ServiceUnavailable{503};
inline constexpr HttpStatusCode GatewayTimeout{504};
inline constexpr HttpStatusCode HttpVersionNotSupported{505};
inline constexpr HttpStatusCode VariantAlsoNegotiates{506};
inline constexpr HttpStatusCode InsufficientStorage{507};
inline constexpr HttpStatusCode LoopDetected{508};
inline constexpr HttpStatusCode NotExtended{510};
inline constexpr HttpStatusCode NetworkAuthenticationRequired{511};
} // namespace HttpStatus
} // namespace AVEVA
