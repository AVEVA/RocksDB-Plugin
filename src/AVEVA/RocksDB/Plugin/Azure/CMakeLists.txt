add_subdirectory(Impl)
add_subdirectory(Models)
add_library(aveva-rocksdb-plugin-azure
    LockFile.cpp
    Directory.cpp
    AzureErrorTranslator.cpp
    Logger.cpp
    ReadableFile.cpp
    WriteableFile.cpp
    ReadWriteFile.cpp
    BlobFilesystem.cpp
    Plugin.cpp
)
add_library(aveva::rocksdb-plugin-azure ALIAS aveva-rocksdb-plugin-azure)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../include")
target_sources(aveva-rocksdb-plugin-azure
  PUBLIC
  FILE_SET HEADERS
  BASE_DIRS "${base-include-dir}"
  FILES
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/LockFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Directory.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
)
find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
target_link_libraries(aveva-rocksdb-plugin-azure PUBLIC
    aveva-rocksdb-plugin-core
    aveva::rocksdb-plugin-azure-impl
    Azure::azure-storage-blobs
    Azure::azure-identity
    RocksDB::rocksdb
)
aveva_install_library(aveva-rocksdb-plugin-azure)
target_compile_features(aveva-rocksdb-plugin-azure PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-azure PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
