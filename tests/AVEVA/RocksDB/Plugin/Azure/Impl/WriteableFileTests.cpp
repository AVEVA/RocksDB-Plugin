#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::Impl::WriteableFileImpl;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using boost::log::sources::logger_mt;

class WriteableFileTests : public ::testing::Test
{
protected:
    std::shared_ptr<BlobClientMock> m_blobClient;
    std::shared_ptr<logger_mt> m_logger;
    WriteableFileImpl m_file;

public:
    WriteableFileTests()
        : m_blobClient(std::make_shared<BlobClientMock>()),
        m_logger(std::make_shared<logger_mt>()),
        m_file("", 0, m_blobClient, nullptr, m_logger)
    {
    }
};
