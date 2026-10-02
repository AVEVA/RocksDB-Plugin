#include "RequestValidation.hpp"

#include <algorithm>
#include <string_view>

namespace AVEVA::Private
{
    bool IsToken(const std::string& value)
    {
        return !value.empty() && std::all_of(value.begin(),
                                     value.end(),
                                     [](unsigned char character)
        {
            return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
                   (character >= '0' && character <= '9') ||
                   std::string_view("!#$%&'*+-.^_`|~").find(character) != std::string_view::npos;
        });
    }
} // namespace AVEVA::Private
