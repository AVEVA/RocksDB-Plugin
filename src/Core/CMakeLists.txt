add_library(aveva-rocksdb-plugin-core
    FileCache.cpp
    FileCacheEntry.cpp
    FileBasedCompressedSecondaryCache.cpp
    ResultHandle.cpp
    LruFileIndex.cpp
    RocksDBHelpers.cpp
    SecondaryCacheFactory.cpp
    Util.cpp
    LocalFilesystem.cpp
    LocalFile.cpp

    # Private headers
    LruFileIndex.hpp
    ResultHandle.hpp
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
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/SecondaryCacheOptions.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/SecondaryCacheFactory.hpp"
)
if(AVEVA_ROCKSDB_WITH_CACHELIB)
    target_sources(aveva-rocksdb-plugin-core
      PRIVATE
        CacheLibSecondaryCache.cpp
      PUBLIC
      FILE_SET HEADERS
      FILES
        "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp"
    )
    find_package(cachelib-navy CONFIG REQUIRED)
    # PRIVATE: CacheLib/folly headers never leak into the public API. Static
    # consumers still get the link dependency via $<LINK_ONLY:...>.
    target_link_libraries(aveva-rocksdb-plugin-core PRIVATE cachelib-navy::cachelib_navy)
    target_compile_definitions(aveva-rocksdb-plugin-core PUBLIC AVEVA_ROCKSDB_WITH_CACHELIB=1)
endif()
install(TARGETS aveva-rocksdb-plugin-core
    EXPORT
)
find_package(boost_algorithm CONFIG REQUIRED)
find_package(boost_container CONFIG REQUIRED)
find_package(boost_scope CONFIG REQUIRED)
find_package(boost_log CONFIG REQUIRED)
find_package(boost_intrusive CONFIG REQUIRED)
find_package(boost_thread CONFIG REQUIRED)
find_package(boost_unordered CONFIG REQUIRED)
find_package(boost_static_string CONFIG REQUIRED)

find_package(RocksDB CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
target_link_libraries(aveva-rocksdb-plugin-core PUBLIC
    Boost::algorithm
    Boost::container
    Boost::scope
    Boost::log
    Boost::intrusive
    Boost::thread
    Boost::unordered
    Boost::static_string
    RocksDB::rocksdb
)
aveva_install_library(aveva-rocksdb-plugin-core)
target_compile_features(aveva-rocksdb-plugin-core PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-core PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
