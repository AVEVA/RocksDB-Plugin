add_library(aveva-rocksdb-plugin-azure-impl
    BlobHelpers.cpp
    PageBlob.cpp
    ReadableFileImpl.cpp
    WriteableFileImpl.cpp
    ReadWriteFileImpl.cpp
    BlobFilesystemImpl.cpp
    StorageAccount.cpp
    LoggerImpl.cpp
    LockFileImpl.cpp
    BlobAttributes.cpp
    DirectoryImpl.cpp
    AzureContainerClient.cpp
)
add_library(aveva::rocksdb-plugin-azure-impl ALIAS aveva-rocksdb-plugin-azure-impl)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../include")
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
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/BlobAttributes.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
)
install(TARGETS aveva-rocksdb-plugin-azure-impl
    EXPORT
)
find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(boost_log CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
target_link_libraries(aveva-rocksdb-plugin-azure-impl PUBLIC
    aveva-rocksdb-plugin-core
    aveva-rocksdb-plugin-azure-models
    Azure::azure-storage-blobs
    Azure::azure-identity
    Boost::log
)
aveva_install_library(aveva-rocksdb-plugin-azure-impl)
target_compile_features(aveva-rocksdb-plugin-azure-impl PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-azure-impl PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
