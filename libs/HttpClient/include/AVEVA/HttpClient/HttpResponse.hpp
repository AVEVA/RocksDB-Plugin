// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpHeader.hpp>

#include <string>
#include <vector>

namespace AVEVA
{
    class HttpResponse
    {
      public:
        HttpResponse();
        HttpResponse(unsigned int status, std::vector<HttpHeader> headers, std::string body);

        unsigned int GetStatus() const noexcept;
        const std::vector<HttpHeader>& GetHeaders() const& noexcept;
        std::vector<HttpHeader> GetHeaders() && noexcept;
        const std::string& GetBody() const& noexcept;
        std::string GetBody() && noexcept;
        void AddHeader(HttpHeader header);

      private:
        unsigned int m_status = 0;
        std::vector<HttpHeader> m_headers;
        std::string m_body;
    };
} // namespace AVEVA
