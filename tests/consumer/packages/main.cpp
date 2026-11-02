#include <AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp>

// The runtime guard keeps the call from executing but still forces the linker to resolve the symbol and
// its transitive dependencies.
int main(int argc, char**) {
    if (argc > 100) {
        AVEVA::RocksDB::Plugin::Azure::Plugin::Unregister(std::string("unused"));
    }
    return 0;
}
