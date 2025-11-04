#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"
namespace AVEVA::RocksDB::Plugin::Core
{
    bool StringEqual::operator()(const std::string& lhs, const std::string& rhs) const
    {
        return lhs == rhs;
    }

    bool StringEqual::operator()(std::string_view lhs, std::string rhs) const
    {
        return lhs == rhs;
    }

    std::size_t StringHash::operator()(const std::string& s) const
    {
        return std::hash<std::string>{}(s);
    }

    std::size_t StringHash::operator()(std::string_view s) const
    {
        return std::hash<std::string_view>{}(s);
    }
}
