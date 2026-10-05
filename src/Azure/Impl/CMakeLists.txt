add_library(aveva-rocksdb-plugin-azure-impl
    BlobHelpers.cpp
    PageBlob.cpp
    ReadableFileImpl.cpp
    WriteableFileImpl.cpp
    ReadWriteFileImpl.cpp
    BlobFilesystemImpl.cpp
    StorageAccount.cpp
    LoggerImpl.cpp
    LogRateLimiter.cpp
    LockFileImpl.cpp
    BlobAttributes.cpp
    DirectoryImpl.cpp
    AzureContainerClient.cpp
    ClientRuntime.cpp
    TokenCredentials.cpp
    Environment.cpp
)
add_library(aveva::rocksdb-plugin-azure-impl ALIAS aveva-rocksdb-plugin-azure-impl)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../../../include")
target_sources(aveva-rocksdb-plugin-azure-impl
  PUBLIC
  FILE_SET HEADERS
  BASE_DIRS "${base-include-dir}"
  FILES
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/BlobAttributes.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/Environment.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
)
install(TARGETS aveva-rocksdb-plugin-azure-impl
    EXPORT
)
find_package(boost_asio CONFIG REQUIRED)
find_package(boost_log CONFIG REQUIRED)
find_package(boost_property_tree CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
target_link_libraries(aveva-rocksdb-plugin-azure-impl PUBLIC
    aveva-rocksdb-plugin-core
    aveva-rocksdb-plugin-azure-models
    aveva::azure-client
    aveva::http-client
    Boost::asio
    Boost::log
)
target_link_libraries(aveva-rocksdb-plugin-azure-impl PRIVATE
    Boost::property_tree
    $<$<PLATFORM_ID:Windows>:crypt32>
)
# Boost.Asio/Winapi/Atomic must agree on the target Windows version across every TU.
target_compile_definitions(aveva-rocksdb-plugin-azure-impl PUBLIC $<$<PLATFORM_ID:Windows>:_WIN32_WINNT=0x0A00>)
aveva_install_library(aveva-rocksdb-plugin-azure-impl)
target_compile_features(aveva-rocksdb-plugin-azure-impl PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-azure-impl PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
