#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "FilesystemMock.hpp"
#include "ContainerClientMock.hpp"
#include "BlobClientMock.hpp"
#include "FileMock.hpp"

#include <gtest/gtest.h>

using boost::log::sources::logger_mt;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using AVEVA::RocksDB::Plugin::Core::FileCache;
using AVEVA::RocksDB::Plugin::Core::FilesystemMock;
using AVEVA::RocksDB::Plugin::Core::ContainerClientMock;
using AVEVA::RocksDB::Plugin::Core::BlobClientMock;
using AVEVA::RocksDB::Plugin::Core::FileMock;
class FileCacheTests : public ::testing::Test
{
protected:
    std::shared_ptr<FilesystemMock> Filesystem;
    std::shared_ptr<ContainerClientMock> ContainerClient;
    std::shared_ptr<logger_mt> Logger;
    FileCache Cache;

public:
    FileCacheTests()
        : Filesystem(std::make_shared<FilesystemMock>()),
        ContainerClient(std::make_shared<ContainerClientMock>()),
        Logger(std::make_shared<logger_mt>()),
        Cache("tmp", static_cast<size_t>(1073741824), ContainerClient, Filesystem, Logger)
    {
    }

    void EnsureReadFromCache(const std::string_view filePath, const size_t expectedCacheSize)
    {
        char buffer[1];
        while (true)
        {
            const auto bytesRead = Cache.ReadFile(filePath, 0, 1, buffer);
            if (bytesRead)
            {
                ASSERT_EQ(expectedCacheSize, Cache.CacheSize());
                break;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }
};

TEST_F(FileCacheTests, ReadEvenSpaced256MbFiles)
{
    const auto fileSize = static_cast<size_t>(268435456);
    EXPECT_CALL(*ContainerClient, GetBlobClient(_))
        .WillRepeatedly(Invoke([fileSize](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize));
                return blob;
            }));

    EXPECT_CALL(*Filesystem, Open(_))
        .WillRepeatedly(Invoke([](const std::filesystem::path&)
            {
                auto file = std::make_unique<FileMock>();
                EXPECT_CALL(*file, Read(_, _, _))
                    .WillRepeatedly(Invoke([](char*, uint64_t offset, uint64_t length)
                        {
                            return length - offset;
                        }));

                return file;
            }));

    EnsureReadFromCache("1.sst", fileSize);
    EnsureReadFromCache("2.sst", fileSize * 2);
    EnsureReadFromCache("3.sst", fileSize * 3);
    EnsureReadFromCache("4.sst", fileSize * 4);
    EnsureReadFromCache("5.sst", fileSize * 4);

    EXPECT_FALSE(Cache.HasFile("1.sst"));
    EXPECT_TRUE(Cache.HasFile("2.sst"));
    EXPECT_TRUE(Cache.HasFile("3.sst"));
    EXPECT_TRUE(Cache.HasFile("4.sst"));
    EXPECT_TRUE(Cache.HasFile("5.sst"));
}
