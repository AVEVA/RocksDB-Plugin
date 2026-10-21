add_library(aveva-rocksdb-plugin-azure-impl STATIC
    AsyncReadRequest.cpp
    AsyncReadTracker.cpp
    BlobHelpers.cpp
    ClientFactory.cpp
    ReadableFileImpl.cpp
    SequentialFileImpl.cpp
    WriteableFileImpl.cpp
    ReadWriteFileImpl.cpp
    BlobFilesystemImpl.cpp
    StorageAccount.cpp
    LoggerImpl.cpp
    LogRateLimiter.cpp
    LockFileImpl.cpp
    BlobAttributes.cpp
    DirectoryImpl.cpp
    ClientRuntime.cpp
    WriteFence.cpp
    BlobMetadataCache.cpp
    PluginClients.cpp
    Environment.cpp
)
add_library(aveva::rocksdb-plugin-azure-impl ALIAS aveva-rocksdb-plugin-azure-impl)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../../../include")
# Only Configuration.hpp is public (Plugin.hpp uses it). Every other header here includes Boost.Asio and the Azure
# client, so they live under include/ in this directory, are not installed, and are visible to in-tree consumers only.
target_sources(aveva-rocksdb-plugin-azure-impl
  PUBLIC
  FILE_SET HEADERS
  BASE_DIRS "${base-include-dir}"
  FILES
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
)
target_include_directories(aveva-rocksdb-plugin-azure-impl PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
find_package(boost_asio CONFIG REQUIRED)
find_package(boost_log CONFIG REQUIRED)
find_package(boost_json CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
target_link_libraries(aveva-rocksdb-plugin-azure-impl PUBLIC
    aveva-rocksdb-plugin-core
    aveva-rocksdb-plugin-azure-models
    aveva::azure-client
    aveva::http-client
    Boost::asio
    Boost::log
    Boost::json
)
target_link_libraries(aveva-rocksdb-plugin-azure-impl PRIVATE
    $<$<PLATFORM_ID:Windows>:crypt32>
)
if(MSVC)
    target_compile_options(aveva-rocksdb-plugin-azure-impl PRIVATE /bigobj)
endif()
aveva_install_library(aveva-rocksdb-plugin-azure-impl)
target_compile_features(aveva-rocksdb-plugin-azure-impl PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-azure-impl PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
