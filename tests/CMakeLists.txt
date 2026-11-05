find_package(GTest CONFIG REQUIRED)
include_directories(.)
add_subdirectory(AVEVA/RocksDB/Plugin/Core)
add_subdirectory(AVEVA/RocksDB/Plugin/Azure/Impl)
