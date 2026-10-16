add_subdirectory(Impl)
add_subdirectory(Models)

# NOTE: Build this target as STATIC because `RocksDB::rocksdb` is
# consumed as a static library. Keeping this target static preserves
# that linkage model and gives downstream consumers more flexibility
# in how they package and link their final binaries.
add_library(aveva-rocksdb-plugin-azure STATIC
    LockFile.cpp
    Directory.cpp
    AzureErrorTranslator.cpp
    Logger.cpp
    ReadableFile.cpp
    SequentialFile.cpp
    WriteableFile.cpp
    ReadWriteFile.cpp
    BlobFilesystem.cpp
    Plugin.cpp
    PluginRegistry.cpp
)
add_library(aveva::rocksdb-plugin-azure ALIAS aveva-rocksdb-plugin-azure)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../../include")
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
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/SequentialFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Defaults.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
)
find_package(RocksDB CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
# The implementation target is linked publicly because this is a static library and its dependencies must reach the
# final link. The public headers themselves only include Defaults.hpp (plain constants), not Asio or the
# Azure client. Boost.Log is part of the public API (loggers are passed to
# Plugin::Register).
find_package(boost_log CONFIG REQUIRED)
target_link_libraries(aveva-rocksdb-plugin-azure
    PUBLIC
        aveva::rocksdb-plugin-core
        RocksDB::rocksdb
        Boost::log
        aveva::rocksdb-plugin-azure-impl
)
if(MSVC)
    target_compile_options(aveva-rocksdb-plugin-azure PRIVATE /bigobj)
endif()
aveva_install_library(aveva-rocksdb-plugin-azure)
target_compile_features(aveva-rocksdb-plugin-azure PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-azure PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
