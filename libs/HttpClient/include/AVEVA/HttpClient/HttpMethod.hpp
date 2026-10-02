#pragma once

#include <string>

namespace AVEVA
{
    enum class HttpMethod
    {
        Get,
        Post,
        Put,
        Patch,
        Delete,
        Head,
        Options,
        Trace,
        Connect
    };

    std::string ToString(HttpMethod method);
} // namespace AVEVA
