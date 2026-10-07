# AVEVA overlay port: enables db_bench + exports db_bench_tool as an installable
# static library so downstream consumers (e.g. the aveva_db_bench wrapper) can
# register the Azure filesystem plugin *before* calling db_bench_tool().
#
# Sync procedure when the vcpkg builtin-baseline is bumped:
#   1. Diff $VCPKG_ROOT/ports/rocksdb against this directory.
#   2. Re-apply the changes below on top of the upstream port.
vcpkg_from_github(
  OUT_SOURCE_PATH SOURCE_PATH
  REPO facebook/rocksdb
  REF "v${VERSION}"
  SHA512 81303c3cb5ababec07d08ff83c5c6bb4f260f5ea3fcbb0f17ee0cc1e54cb41c0c95593face68988d1ca4fb2b5de6e4ca5b2e972d2fffd524e441b88063968696
  HEAD_REF main
  PATCHES
    0001-fix-dependencies.patch
    0002-fix-android.patch
    0004-install-db-bench-tool-lib.patch
)

string(COMPARE EQUAL "${VCPKG_CRT_LINKAGE}" "dynamic" WITH_MD_LIBRARY)
string(COMPARE EQUAL "${VCPKG_LIBRARY_LINKAGE}" "dynamic" ROCKSDB_BUILD_SHARED)

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
  FEATURES
    "liburing" WITH_LIBURING
    "snappy" WITH_SNAPPY
    "lz4" WITH_LZ4
    "zlib" WITH_ZLIB
    "zstd" WITH_ZSTD
    "bzip2" WITH_BZ2
    "numa" WITH_NUMA
    "tbb" WITH_TBB
    "db-bench" WITH_DB_BENCH
)

if(WITH_LIBURING)
  vcpkg_find_acquire_program(PKGCONFIG)
  vcpkg_list(APPEND FEATURE_OPTIONS "-DPKG_CONFIG_EXECUTABLE=${PKGCONFIG}")
endif()

vcpkg_cmake_configure(
  SOURCE_PATH "${SOURCE_PATH}"
  OPTIONS
    # db_bench_tool (installed by 0004-install-db-bench-tool-lib.patch) is gated
    # on WITH_GFLAGS, so gflags + that static lib are only built when the
    # "db-bench" feature is active. WITH_BENCHMARK_TOOLS stays OFF: it also
    # builds table_reader_bench/memtablerep_bench/etc. which pull in gtest and
    # break the Windows build (see 0004 patch notes).
    -DWITH_GFLAGS=${WITH_DB_BENCH}
    -DWITH_TESTS=OFF
    -DWITH_BENCHMARK_TOOLS=OFF
    -DWITH_TOOLS=OFF
    -DUSE_RTTI=ON
    -DROCKSDB_INSTALL_ON_WINDOWS=ON
    -DFAIL_ON_WARNINGS=OFF
    -DWITH_MD_LIBRARY=${WITH_MD_LIBRARY}
    -DPORTABLE=1 # Minimum CPU arch to support, or 0 = current CPU, 1 = baseline CPU
    -DROCKSDB_BUILD_SHARED=${ROCKSDB_BUILD_SHARED}
    -DCMAKE_DISABLE_FIND_PACKAGE_Git=TRUE
    ${FEATURE_OPTIONS}
  OPTIONS_DEBUG
    -DCMAKE_DEBUG_POSTFIX=d
    -DWITH_RUNTIME_DEBUG=ON
  OPTIONS_RELEASE
    -DWITH_RUNTIME_DEBUG=OFF
)

vcpkg_cmake_install()

vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/rocksdb)

vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")

vcpkg_fixup_pkgconfig()

vcpkg_install_copyright(COMMENT [[
RocksDB is dual-licensed under both the GPLv2 (found in COPYING)
and Apache 2.0 License (found in LICENSE.Apache). You may select,
at your option, one of the above-listed licenses.
]]
  FILE_LIST
    "${SOURCE_PATH}/LICENSE.leveldb"
    "${SOURCE_PATH}/LICENSE.Apache"
    "${SOURCE_PATH}/COPYING"
)
