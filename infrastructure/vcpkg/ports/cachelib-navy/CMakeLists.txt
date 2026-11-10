# Port-owned build for the thrift-free CacheLib Navy subset.
# Copied to <CacheLib source>/aveva-navy/CMakeLists.txt by portfile.cmake.
cmake_minimum_required(VERSION 3.22)
project(cachelib-navy LANGUAGES CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 20)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(Threads REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)
find_package(magic_enum CONFIG REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(tsl-sparse-map CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS regex)

get_filename_component(CACHELIB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
set(CL "${CACHELIB_ROOT}/cachelib")
set(SHIM "${CMAKE_CURRENT_SOURCE_DIR}/shim")

add_library(cachelib_navy STATIC
  ${CL}/allocator/nvmcache/NavyConfig.cpp
  ${CL}/allocator/nvmcache/NavySetup.cpp
  ${CL}/allocator/nvmcache/NvmItem.cpp
  ${CL}/common/BloomFilter.cpp
  ${CL}/common/EventTracker.cpp
  ${CL}/common/FurcHash.cpp
  ${CL}/common/inject_pause.cpp
  ${CL}/common/PercentileStats.cpp
  ${CL}/common/Serialization.cpp
  ${CL}/common/Utils.cpp
  ${CL}/shm/PosixShmSegment.cpp
  ${CL}/shm/ShmCommon.cpp
  ${CL}/shm/ShmManager.cpp
  ${CL}/shm/SysVShmSegment.cpp
  ${CL}/navy/admission_policy/DynamicRandomAP.cpp
  ${CL}/navy/admission_policy/RejectRandomAP.cpp
  ${CL}/navy/bighash/BigHash.cpp
  ${CL}/navy/bighash/Bucket.cpp
  ${CL}/navy/bighash/BucketStorage.cpp
  ${CL}/navy/block_cache/Allocator.cpp
  ${CL}/navy/block_cache/BlockCache.cpp
  ${CL}/navy/block_cache/CombinedEntryBlock.cpp
  ${CL}/navy/block_cache/FifoPolicy.cpp
  ${CL}/navy/block_cache/FixedSizeIndex.cpp
  ${CL}/navy/block_cache/HitsReinsertionPolicy.cpp
  ${CL}/navy/block_cache/LruPolicy.cpp
  ${CL}/navy/block_cache/Region.cpp
  ${CL}/navy/block_cache/RegionManager.cpp
  ${CL}/navy/block_cache/ReuseTimeReinsertionPolicy.cpp
  ${CL}/navy/block_cache/SparseMapIndex.cpp
  ${CL}/navy/common/Buffer.cpp
  ${CL}/navy/common/Device.cpp
  ${CL}/navy/common/FdpNvme.cpp
  ${CL}/navy/common/Hash.cpp
  ${CL}/navy/common/NavyThread.cpp
  ${CL}/navy/common/SizeDistribution.cpp
  ${CL}/navy/common/Types.cpp
  ${CL}/navy/driver/Driver.cpp
  ${CL}/navy/engine/EnginePair.cpp
  ${CL}/navy/Factory.cpp
  ${CL}/navy/scheduler/NavyRequestDispatcher.cpp
  ${CL}/navy/scheduler/NavyRequestScheduler.cpp
  ${CL}/navy/scheduler/ThreadPoolJobScheduler.cpp
  ${CL}/navy/scheduler/ThreadPoolJobQueue.cpp
  ${CL}/navy/serialization/RecordIO.cpp
)
add_library(cachelib-navy::cachelib_navy ALIAS cachelib_navy)

# Shim headers must win over any real thrift headers on the include path.
target_include_directories(cachelib_navy BEFORE PUBLIC
  $<BUILD_INTERFACE:${SHIM}>
  $<BUILD_INTERFACE:${CACHELIB_ROOT}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# Navy's io_uring/libaio engines need folly built with FOLLY_HAS_LIBURING, which the
# vcpkg folly port never sets; Navy uses its synchronous thread-pool I/O path.
target_compile_definitions(cachelib_navy PUBLIC CACHELIB_IOURING_DISABLE)
# Log categories become "cachelib.navy..." instead of the absolute build path,
# so consumers can tune them with folly::LoggerDB::setLevel("cachelib", ...).
target_compile_definitions(cachelib_navy PRIVATE "XLOG_STRIP_PREFIXES=\"${CACHELIB_ROOT}\"")

target_link_libraries(cachelib_navy
  PUBLIC
    Folly::folly
    fmt::fmt
    glog::glog
    magic_enum::magic_enum
    tsl::sparse_map
    Threads::Threads
  PRIVATE
    xxHash::xxhash
    Boost::regex
)

install(TARGETS cachelib_navy EXPORT cachelib-navy-targets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

foreach(dir navy common shm allocator)
  install(DIRECTORY ${CL}/${dir}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cachelib
    FILES_MATCHING PATTERN "*.h"
    PATTERN "tests" EXCLUDE
    PATTERN "testing" EXCLUDE
    PATTERN "benchmarks" EXCLUDE
    PATTERN "facebook" EXCLUDE)
endforeach()
install(DIRECTORY ${SHIM}/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT cachelib-navy-targets
  NAMESPACE cachelib-navy::
  DESTINATION ${CMAKE_INSTALL_DATADIR}/cachelib-navy)
configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cachelib-navy-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/cachelib-navy-config.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_DATADIR}/cachelib-navy)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cachelib-navy-config.cmake
  DESTINATION ${CMAKE_INSTALL_DATADIR}/cachelib-navy)
