// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "LocalIoUtil.hpp"

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/scope/scope_exit.hpp>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#else
#  include <fcntl.h>
#  include <unistd.h>
#endif

namespace AVEVA::RocksDB::Plugin::Core
{
    namespace bip = boost::interprocess;

    /// <summary>Concrete MappedFileView wrapping a boost::interprocess::mapped_region.</summary>
    class BoostMappedFileView final : public MappedFileView
    {
    public:
        // Takes ownership of both objects; mapped_region keeps the OS handle
        // independently of file_mapping, so the file_mapping can go out of scope.
        BoostMappedFileView(bip::file_mapping mapping, bip::mapped_region region)
            : m_mapping(std::move(mapping)), m_region(std::move(region)) {
        }

        const char* Data() const noexcept override
        {
            return static_cast<const char*>(m_region.get_address());
        }
        size_t Size() const noexcept override { return m_region.get_size(); }

    private:
        bip::file_mapping  m_mapping;
        bip::mapped_region m_region;
    };

    bool LocalIoUtil::WriteAllBytesToFile(const std::filesystem::path& path,
        const char* data, size_t size) noexcept
    {
#ifdef _WIN32
        HANDLE h = ::CreateFileW(path.wstring().c_str(),
            GENERIC_WRITE, 0, nullptr,
            CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
        if (h == INVALID_HANDLE_VALUE) return false;
        auto handleCleanup = boost::scope::make_scope_exit([h] noexcept { ::CloseHandle(h); });
        const char* ptr = data;
        size_t remaining = size;
        while (remaining > 0)
        {
            const DWORD chunk = static_cast<DWORD>(
                remaining > MAXDWORD ? MAXDWORD : remaining);
            DWORD written = 0;
            if (!::WriteFile(h, ptr, chunk, &written, nullptr) || written != chunk)
                return false;
            ptr += written;
            remaining -= written;
        }
        return true;
#else
        const int fd = ::open(path.string().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) return false;
        auto fdCleanup = boost::scope::make_scope_exit([fd] noexcept { ::close(fd); });
        const char* ptr = data;
        size_t remaining = size;
        while (remaining > 0)
        {
            const ssize_t written = ::write(fd, ptr, remaining);
            if (written <= 0) return false;
            ptr += written;
            remaining -= static_cast<size_t>(written);
        }
        return true;
#endif
    }

    std::unique_ptr<MappedFileView> LocalIoUtil::MapFileReadOnly(
        const std::filesystem::path& path) noexcept
    {
        try
        {
            bip::file_mapping fm(path.string().c_str(), bip::read_only);
            bip::mapped_region region(fm, bip::read_only);
            return std::make_unique<BoostMappedFileView>(std::move(fm), std::move(region));
        }
        catch (...)
        {
            return nullptr;
        }
    }
}
