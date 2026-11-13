#include <AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

// Plugin::UniquePrefix is a public API; the link has to resolve it from the installed library.
int main(int argc, char**) {
    if (argc > 100) {
        return static_cast<int>(AVEVA::RocksDB::Plugin::Azure::Plugin::UniquePrefix(
                                    AVEVA::RocksDB::Plugin::Azure::Models::AccountUrl{"https://a.b/"},
                                    AVEVA::RocksDB::Plugin::Azure::Models::DbName{"c"})
                                    .size());
    }
    return AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::MaxClientRetries >
                   AVEVA::RocksDB::Plugin::Azure::Impl::RetryCount{0}
               ? 0
               : 1;
}
