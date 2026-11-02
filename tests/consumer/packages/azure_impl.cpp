#include <AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

// Plugin::UniquePrefix is a public API; the link has to resolve it from the installed library.
int main(int argc, char**) {
    if (argc > 100) {
        return static_cast<int>(AVEVA::RocksDB::Plugin::Azure::Plugin::UniquePrefix("https://a.b/", "c").size());
    }
    return AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::MaxClientRetries > 0 ? 0 : 1;
}
