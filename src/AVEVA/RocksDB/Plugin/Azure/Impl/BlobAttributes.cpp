#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobAttributes.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    BlobAttributes::BlobAttributes(const uint64_t size, std::string name)
        : m_size(size), m_name(std::move(name))
    {
    }

    uint64_t BlobAttributes::GetSize() const noexcept
    {
        return m_size;
    }

    const std::string& BlobAttributes::GetName() const noexcept
    {
        return m_name;
    }
}
