add_subdirectory(Mocks)
add_executable(aveva-rocksdb-plugin-core-tests
    CoreTests.cpp
    FileCacheTests.cpp
    FileBasedCompressedSecondaryCacheRoundTripTests.cpp
    FileBasedCompressedSecondaryCacheCapacityTests.cpp
    FileBasedCompressedSecondaryCacheCompressionTests.cpp
    FileBasedCompressedSecondaryCacheCorruptionTests.cpp
    FileBasedCompressedSecondaryCacheApiTests.cpp
    FileBasedCompressedSecondaryCacheConcurrencyTests.cpp
    FileBasedCompressedSecondaryCacheExceptionTests.cpp
    FileBasedCompressedSecondaryCacheMockTests.cpp
    SecondaryCacheRecordFormatTests.cpp
    SecondaryCacheRegionCorruptionTests.cpp
    SecondaryCacheShardedIndexTests.cpp
    SecondaryCacheRegionManagerTests.cpp
    SecondaryCacheRegionEngineTests.cpp
)

target_link_libraries(aveva-rocksdb-plugin-core-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-core aveva-rocksdb-plugin-core-mocks)
target_include_directories(aveva-rocksdb-plugin-core-tests PRIVATE ${CMAKE_SOURCE_DIR}/src/Core)
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-core-tests)

add_executable(aveva-secondary-cache-benchmark SecondaryCacheBenchmark.cpp)
target_link_libraries(aveva-secondary-cache-benchmark PRIVATE aveva-rocksdb-plugin-core)
