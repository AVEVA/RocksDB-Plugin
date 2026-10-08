add_library(aveva-rocksdb-plugin-core-mocks
    FilesystemMock.cpp
    FilesystemMock.hpp
    ContainerClientMock.cpp
    ContainerClientMock.hpp
    BlobClientMock.cpp
    BlobClientMock.hpp
    FileMock.hpp
    FileMock.cpp
)
target_link_libraries(aveva-rocksdb-plugin-core-mocks PUBLIC GTest::gmock aveva-rocksdb-plugin-core)

