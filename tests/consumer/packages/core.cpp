#include <AVEVA/RocksDB/Plugin/Core/Util.hpp>

int main(int argc, char**) {
    if (argc > 100) {
        return static_cast<int>(AVEVA::RocksDB::Plugin::Core::StringHash{}(std::string_view("unused")));
    }
    return 0;
}
