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
    SecondaryCacheContractTests.cpp
    SecondaryCacheFactoryTests.cpp
    SecondaryCacheRocksDbTests.cpp
)
if(AVEVA_ROCKSDB_WITH_CACHELIB)
    target_sources(aveva-rocksdb-plugin-core-tests PRIVATE CacheLibSecondaryCacheTests.cpp)
endif()

target_link_libraries(aveva-rocksdb-plugin-core-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-core aveva-rocksdb-plugin-core-mocks)
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-core-tests)
