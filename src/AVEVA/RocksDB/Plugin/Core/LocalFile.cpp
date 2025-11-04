#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"
namespace AVEVA::RocksDB::Plugin::Core
{
    LocalFile::LocalFile(const std::filesystem::path& path)
        : m_file(path, std::ios::in | std::ios::binary)
    {
    }

    uint64_t LocalFile::Read(char* buffer, uint64_t offset, uint64_t length)
    {
        m_file.seekg(static_cast<std::streamoff>(offset));
        m_file.read(buffer, static_cast<std::streamsize>(length));

        const auto bytesRead = m_file.gcount();
        if (bytesRead < 0)
        {
            throw std::runtime_error("Invalid file read request. Received negative number of bytes read.");
        }

        return static_cast<uint64_t>(bytesRead);
    }
}
