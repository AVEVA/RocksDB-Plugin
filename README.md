# AVEVA RocksDB Plugins

This project contains AVEVA's plugins for the [RocksDB](https://rocksdb.org/) database that bring RocksDB to Azure Cloud infrastructure. This plugin leverages the [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp) and Azure Blob Storage to provide a seamless, cloud-native storage solution for RocksDB applications.

## Plugin List

Plugins are compiled with RocksDB and can be optionally enabled at runtime. To learn more about RocksDB
plugins, please check out RocksDB's documentation on [building plugins](https://github.com/facebook/rocksdb/blob/main/plugin/README.md)
and the list of [known plugins](https://github.com/facebook/rocksdb/blob/main/PLUGINS.md) that are being developed. Below is the list of plugins that exist in this repository and are actively being developed and maintained by AVEVA.

### [Azure Page Blob Filesystem](src/AVEVA/RocksDB/Plugin/Azure)

This plugin uses the [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp) for interacting with page blobs
in place of a local filesystem. This allows a service using RocksDB to be deployed in a "stateless" manner
(e.g., Kubernetes Pod, Service Fabric stateless service, etc.) and connect to an Azure blob container
for all of its storage needs. This is similar to using
[SMB network shares](https://learn.microsoft.com/en-us/windows-server/storage/file-server/file-server-smb-overview)
or a FUSE filesystem (if deploying on Linux where [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) is supported).
However, a fully integrated solution allows for purpose tuned performance for RocksDB, self-contained deployments (as opposed to having to configure SMB shares or FUSE on the host system), and a whole host of other useful things.

#### Why Use The Blob Storage Filesystem?

Using a blob container instead of a local filesystem enables modern cloud-native deployments by addressing key challenges faced when running RocksDB in containerized and stateless environments:

- **Stateless Deployment**: Deploy RocksDB applications in containerized environments (Kubernetes Pods, Service Fabric stateless services) without depending on persistent local storage
- **Cloud-Native Storage**: Utilize Azure Blob Storage as the primary storage backend, eliminating the need for complex storage provisioning and management
- **High Availability**: Built-in data durability and redundancy through Azure's globally distributed storage infrastructure
- **Cost Optimization**: Leverage Azure's tiered storage options and pay-per-use model instead of pre-provisioning expensive local SSDs
- **Performance Optimized**: Purpose-built integration with RocksDB internals provides better performance than generic network filesystem solutions like SMB or FUSE
- **Self-Contained**: No host-level configuration required - everything needed is packaged within your application

This is particularly valuable for enterprise applications that need the performance of RocksDB with the operational benefits of cloud storage, enabling stateless microservices architectures.

#### How to Use

##### Prerequisites

Before using the AVEVA RocksDB Azure Plugin, ensure you have:

1. **Azure Storage Account**: Create an Azure Storage Account with blob storage enabled
2. **Authentication**: Configure authentication using one of the following methods:
   - **Service Principal**: See [Microsoft's documentation on accessing Storage Accounts with Service Principals](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/aad-storage-service-principal)
   - **Managed Identity**: See [Microsoft's documentation on accessing Storage Accounts with Managed Identity](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities)
   - **Connection String**: For development and testing
3. **RocksDB**: Build RocksDB with plugin support enabled

###### Basic Usage

1. **Register the Plugin**: Initialize the Azure filesystem plugin in your application:
    ```cpp
    #include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>
    #include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
    #include <boost/log/sources/logger.hpp>
    #include <memory>

    using AVEVA::RocksDB::Plugin::Azure::Plugin;
    using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard = nullptr;
    ServicePrincipalStorageInfo storageCredentials
    {
        "blobContainerPath",
        "azureAccountURL",
        "servicePrincipalID",
        "servicePrincipalSecret",
        "tenantID"
    };
    
    rocksdb::Status status = Plugin::Register(primaryDbOptions,
        &env,
        &guard,
        storageCredentials,
        std::nullopt, /* backup credentials */
        std::make_shared<boost::log::sources::logger_mt>(),
        MbToBytes(2), /* dataFileBufferSize */
        MbToBytes(4) /* dataFileInitialSize */);
    ```

2. **Open RocksDB with Azure Storage**: 
    ```cpp
    using AVEVA::RocksDB::Plugin::Azure::Impl::StorageAccount;

    rocksdb::Options options;
    options.env = env; /* From step 1. */
   
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::Status status = rocksdb::DB::Open(options,
        StorageAccount::UniquePrefix("azureAccountURL", "blobContainerName"),
        &db);
   ```

3. **Use RocksDB Normally**: Once configured, use RocksDB APIs as you normally would - all data will be transparently stored in Azure Blob Storage.

###### Configuration Options

- **Caching**: Enable local caching for frequently accessed data. SSTs are cached on repeat access, which avoids eager one-time startup scans downloading every SST.
- **Bounded stale metadata**: Never-downloaded stale SST entries are capped and oldest metadata-only entries are pruned to prevent unbounded cache index growth.

For detailed configuration examples and advanced usage patterns, see the [Azure Plugin Documentation](src/AVEVA/RocksDB/Plugin/Azure/README.md).

### Secondary cache

On Linux x64, CacheLib's Navy BlockCache can be used as a secondary cache (`CacheLibSecondaryCache`).
Blocks evicted from RocksDB's primary cache can be stored in the secondary cache, adding another layer before hitting the
file cache or an expensive Azure read. It uses only CacheLib's SSD engine (Navy), with no CacheLib memory tier, because
RocksDB's block cache already serves as the in-memory tier. Supports region FIFO (default) or LRU eviction.

#### Usage
Set `AVEVA_ROCKSDB_WITH_CACHELIB=ON`, guarding its use with `#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)`.
The constructor throws `std::invalid_argument` on invalid options. Since CacheLib is only supported on linux, 
there is no fallback secondary cache option for Windows.

```cpp
#include <AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp>

using namespace AVEVA::RocksDB::Plugin::Core;
SecondaryCacheOptions secondary;
secondary.cacheDir = "/mnt/nvme/rocksdb-cache";
secondary.capacity = 64ULL << 30;
secondary.logger = std::make_shared<SecondaryCacheOptions::Logger>();

rocksdb::LRUCacheOptions primary;
primary.capacity = 1ULL << 30;
primary.secondary_cache = std::make_shared<CacheLibSecondaryCache>(secondary);
table_options.block_cache = primary.MakeSharedCache();
```

Memory use is small, but it grows with the number of cached entries. See [SecondaryCacheOptions.hpp](include/AVEVA/RocksDB/Plugin/Core/SecondaryCacheOptions.hpp)
for tuning and the [Navy overlay port](infrastructure/vcpkg/ports/cachelib-navy/README.md#memory-use) for a breakdown.

## Building

1. Ensure that vcpkg is installed and in the PATH
    * Clone the repo `https://github.com/microsoft/vcpkg.git` into some `<vcpkg-path>`
    * Run `<vcpkg-path>/bootstrap-vcpkg` bat or shell script depending on your platform
    * Set environment variable `VCPKG_ROOT` to `<vcpkg-path>`
    * Add `VCPKG_ROOT` to `PATH` environment variable
    * Reload shell to update environment variables
2. Clone the repo `https://github.com/AVEVA/RocksDB-Plugin.git` into some `<rocksdb-plugin-path>`
3. Configure the project (e.g., `cmake -S <rocksdb-plugin-path> -B <rocksdb-plugin-build-path> --preset [Windows/Linux][Debug/Release]`)
    * Presets can be found in [CMakePresets.json](CMakePresets.json)
4. Build the project (e.g., `cmake --build <rocksdb-plugin-build-path>`)

## Dependency update automation

This repository uses [Dependabot](https://docs.github.com/en/code-security/dependabot) to automate dependency updates. Configuration lives in [`.github/dependabot.yml`](.github/dependabot.yml) and covers:

- **vcpkg** — bumps the `builtin-baseline` in [`vcpkg.json`](vcpkg.json).
- **GitHub Actions** — keeps workflow action versions current.

Dependency update PRs should only be merged after CI checks pass.

## Contributing

We are not accepting PRs from anyone outside of the AVEVA organization currently.
Please create an issue for proposed changes.
