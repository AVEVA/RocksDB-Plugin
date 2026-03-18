add_library(aveva-rocksdb-plugin-core
    FileCache.cpp
    FileCacheEntry.cpp
    FileBasedCompressedSecondaryCache.cpp
    ResultHandle.cpp
    LruFileIndex.cpp
    RocksDBHelpers.cpp
    Util.cpp
    LocalFilesystem.cpp
    LocalFile.cpp
    LocalIoUtil.cpp
    CrcUtil.cpp
    ZstdCodec.cpp

    # Private headers
    LruFileIndex.hpp
    MapEntryResult.hpp
    ParsedHeader.hpp
    FileFormat.hpp
    ResultHandle.hpp
    CrcUtil.hpp
    ZstdCodec.hpp
)
add_library(aveva::rocksdb-plugin-core ALIAS aveva-rocksdb-plugin-core)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../../include")
target_sources(aveva-rocksdb-plugin-core
  PUBLIC
  FILE_SET HEADERS
  BASE_DIRS "${base-include-dir}"
  FILES
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/File.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/Util.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"
)
install(TARGETS aveva-rocksdb-plugin-core
    EXPORT
)
find_package(boost_algorithm CONFIG REQUIRED)
find_package(boost_container CONFIG REQUIRED)
find_package(boost_crc CONFIG REQUIRED)
find_package(boost_interprocess CONFIG REQUIRED)
find_package(boost_iostreams CONFIG REQUIRED)
find_package(boost_scope CONFIG REQUIRED)
find_package(boost_log CONFIG REQUIRED)
find_package(boost_intrusive CONFIG REQUIRED)
find_package(boost_endian CONFIG REQUIRED)
find_package(boost_thread CONFIG REQUIRED)
find_package(boost_unordered CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
target_link_libraries(aveva-rocksdb-plugin-core PUBLIC
    Boost::algorithm
    Boost::container
    Boost::crc
    Boost::interprocess
    Boost::scope
    Boost::log
    Boost::intrusive
    Boost::endian
    Boost::thread
    Boost::unordered
    RocksDB::rocksdb
    zstd::libzstd
)
aveva_install_library(aveva-rocksdb-plugin-core)
target_compile_features(aveva-rocksdb-plugin-core PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-core PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
