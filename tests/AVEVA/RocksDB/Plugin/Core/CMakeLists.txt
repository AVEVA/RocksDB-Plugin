add_executable(aveva-rocksdb-plugin-core-tests
    CoreTests.cpp
    FileCacheTests.cpp
    FilesystemMock.cpp
    FilesystemMock.hpp
    ContainerClientMock.cpp
    ContainerClientMock.hpp
    BlobClientMock.cpp
    BlobClientMock.hpp
    FileMock.hpp
    FileMock.cpp
)

target_link_libraries(aveva-rocksdb-plugin-core-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-core)
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-core-tests)
