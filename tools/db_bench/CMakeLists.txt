find_package(RocksDB CONFIG REQUIRED)

add_executable(aveva_db_bench main.cpp)
target_compile_features(aveva_db_bench PRIVATE cxx_std_23)
target_link_libraries(aveva_db_bench PRIVATE
    aveva::rocksdb-plugin-azure
    RocksDB::rocksdb
    RocksDB::db_bench_tool
)

# Match the RocksDB executable naming so users familiar with upstream
# db_bench can invoke this wrapper the same way.
set_target_properties(aveva_db_bench PROPERTIES OUTPUT_NAME "db_bench")
