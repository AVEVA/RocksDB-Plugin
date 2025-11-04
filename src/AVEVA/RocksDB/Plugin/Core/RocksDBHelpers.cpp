#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
namespace AVEVA::RocksDB::Plugin::Core
{
    static bool IsFile(const std::string& pathname, const std::string_view file)
    {
        static const constexpr std::string_view pathsep{ "/" };

        // extract last component of the path
        std::string fname;
        size_t offset = pathname.find_last_of(pathsep);
        if (offset != std::string::npos)
        {
            fname = pathname.substr(offset + 1, pathname.size());
        }
        else
        {
            fname = pathname;
        }
        if (fname.find(file) == 0)
        {
            return true;
        }
        return false;
    }

    bool RocksDBHelpers::IsManifestFile(const std::string& pathname)
    {
        return IsFile(pathname, "MANIFEST");
    }

    bool RocksDBHelpers::IsIdentityFile(const std::string& pathname)
    {
        return IsFile(pathname, "IDENTITY");
    }

    bool RocksDBHelpers::IsLogFile(const RocksDBHelpers::FileClass fileType)
    {
        // A log file has ".log" suffix or starts with 'MANIFEST"
        switch (fileType)
        {
        case FileClass::WAL:
        case FileClass::Manifest:
            return true;
        default:
            return false;
        }
    }

    RocksDBHelpers::FileClass RocksDBHelpers::GetFileType(const std::string& pathname)
    {
        // Is this a sst file, i.e. ends in ".sst" or ".ldb"
        if (pathname.ends_with(FileType::sst) || pathname.ends_with(FileType::ldb))
        {
            return FileClass::SST;
        }

        // A log file has ".log" suffix
        if (pathname.ends_with(FileType::log))
        {
            return FileClass::WAL;
        }

        if (IsManifestFile(pathname))
        {
            return FileClass::Manifest;
        }

        if (IsIdentityFile(pathname))
        {
            return FileClass::Identity;
        }

        return FileClass::Directory;
    }
}
