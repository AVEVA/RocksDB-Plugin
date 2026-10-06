#include "RequestValidation.hpp"

#include <algorithm>
#include <boost/beast/http/fields.hpp>
#include <boost/url/scheme.hpp>
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

    bool IsValidHeader(const std::string& name, const std::string& value)
    {
        return IsToken(name) && name.size() <= boost::beast::http::fields::max_name_size &&
               value.size() <= boost::beast::http::fields::max_value_size &&
               std::none_of(value.begin(),
                   value.end(),
                   [](unsigned char character)
        {
            return (character < 32 && character != '\t') || character == 127;
        });
    }

    bool IsValidRequestUrl(const boost::urls::url_view& url)
    {
        if (!url.has_authority() || url.host().empty() || url.has_userinfo() ||
            (url.scheme_id() != boost::urls::scheme::http && url.scheme_id() != boost::urls::scheme::https) ||
            (url.has_port() && url.port_number() == 0))
        {
            return false;
        }
        const std::string host = url.host_address();
        return std::none_of(host.begin(),
            host.end(),
            [](unsigned char character)
        {
            return character <= 32 || character >= 127;
        });
    }
} // namespace AVEVA::Private
