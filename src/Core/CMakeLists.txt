add_library(aveva-rocksdb-plugin-core
    FileCache.cpp
    FileCacheMaintenance.cpp
    FileCacheEntry.cpp
    FileBasedCompressedSecondaryCache.cpp
    ResultHandle.cpp
    LruFileIndex.cpp
    RocksDBHelpers.cpp
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
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/File.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/Util.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"
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
# PUBLIC only for libraries whose headers appear in installed Core headers; the rest are implementation details.
# The package config still calls find_dependency for all of them because a static build re-exports PRIVATE
# dependencies as $<LINK_ONLY:...> link requirements.
target_link_libraries(aveva-rocksdb-plugin-core
    PUBLIC
        Boost::log
        Boost::intrusive
        Boost::static_string
        RocksDB::rocksdb
    PRIVATE
        Boost::algorithm
        Boost::container
        Boost::unordered
        Boost::scope
        Boost::thread
)
if(MSVC)
    target_compile_options(aveva-rocksdb-plugin-core PRIVATE /bigobj)
endif()
aveva_install_library(aveva-rocksdb-plugin-core)
target_compile_features(aveva-rocksdb-plugin-core PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-core PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
