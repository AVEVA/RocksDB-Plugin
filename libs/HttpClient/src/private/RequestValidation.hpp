#pragma once

#include <string>

namespace AVEVA::Private
{
    // Checks whether value is a valid HTTP token (RFC 7230 section 3.2.6), as required for header names.
    bool IsToken(const std::string& value);
} // namespace AVEVA::Private
