#pragma once

#include <system_error>

namespace AVEVA::Private
{
    class HttpErrorCategory final : public std::error_category
    {
      public:
        const char* name() const noexcept override;
        std::string message(int value) const override;
    };
} // namespace AVEVA::Private
