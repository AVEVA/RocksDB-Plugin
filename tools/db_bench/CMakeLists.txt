# db_bench_tool (from the overlay's 0004 patch) PUBLIC-links gflags, so the
# gflags targets must be importable before RocksDB's exported config is loaded.
find_package(gflags CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
if(NOT TARGET RocksDB::db_bench_tool)
    message(FATAL_ERROR
        "RocksDB::db_bench_tool is missing: the installed RocksDB was not built from the patched overlay port. "
        "Set VCPKG_OVERLAY_PORTS=infrastructure/vcpkg-overlays (or configure with "
        "-DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON from the top level) and reconfigure with a fresh build directory.")
endif()

add_executable(aveva_db_bench BenchmarkRunner.cpp)
target_compile_features(aveva_db_bench PRIVATE cxx_std_23)
if(MSVC)
    target_compile_options(aveva_db_bench PRIVATE /bigobj)
endif()
target_link_libraries(aveva_db_bench PRIVATE
    aveva::rocksdb-plugin-azure
    RocksDB::rocksdb
    RocksDB::db_bench_tool
)

# Match the RocksDB executable naming so users familiar with upstream
# db_bench can invoke this wrapper the same way.
set_target_properties(aveva_db_bench PROPERTIES OUTPUT_NAME "db_bench")
