#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

// The runtime guard keeps the call from executing but still forces the linker to resolve the symbol and
// its transitive dependencies.
int main(int argc, char**) {
    if (argc > 100) {
        namespace Azure = AVEVA::RocksDB::Plugin::Azure;
        Azure::Plugin::Unregister(Azure::Plugin::AccountPair<Azure::Models::ServicePrincipalStorageInfo>{
            Azure::Models::ServicePrincipalStorageInfo{
                Azure::Models::DbName{"db"}, Azure::Models::AccountUrl{"url"}, Azure::Models::ClientId{"client"},
                Azure::Models::ClientSecret{"secret"}, Azure::Models::TenantId{"tenant"}},
            std::nullopt});
    }
    return 0;
}
