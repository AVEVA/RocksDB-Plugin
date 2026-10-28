# AVEVA RocksDB Plugins

This project contains AVEVA's plugins for the [RocksDB](https://rocksdb.org/) database that bring RocksDB to Azure Cloud infrastructure. This plugin leverages AVEVA's in-house Azure Blob Storage client ([libs/AzureClient](libs/AzureClient), built on [libs/HttpClient](libs/HttpClient) and Boost.Asio) and Azure Blob Storage to provide a seamless, cloud-native storage solution for RocksDB applications.

## Plugin List

### [Azure Page Blob Filesystem](src/Azure)

This plugin uses the AVEVA Azure client library ([libs/AzureClient](libs/AzureClient)) for interacting with page blobs
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
 3. **RocksDB**: Build RocksDB with plugin support enabled

###### Basic Usage

1. **Register the Plugin**: Initialize the Azure filesystem plugin in your application:
    ```cpp
    #include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>
    #include <boost/asio/executor_work_guard.hpp>
    #include <boost/asio/io_context.hpp>
    #include <boost/log/sources/severity_logger.hpp>
    #include <boost/log/trivial.hpp>
    #include <memory>
    #include <optional>
    #include <rocksdb/convenience.h>
    #include <rocksdb/env.h>
    #include <thread>

    using AVEVA::RocksDB::Plugin::Azure::Plugin;
    using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

    // The application owns the io_context the plugin performs its Azure I/O on. Keep it alive and running
    // (on threads that never call into RocksDB) for as long as the filesystem is in use.
    boost::asio::io_context ioContext;
    auto workGuard = boost::asio::make_work_guard(ioContext);
    std::thread ioThread{[&ioContext] { ioContext.run(); }};
    // On shutdown, in this order: close the database and release the Env, then `workGuard.reset();`
    // so that ioContext.run() can return, and only then `ioThread.join();`. Joining first never returns.

    constexpr auto MbToBytes = [](int64_t mb) { return mb * 1024 * 1024; };
    rocksdb::ConfigOptions primaryDbOptions;
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
        ioContext,
        storageCredentials,
        std::nullopt, /* backup credentials */
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(),
        MbToBytes(2), /* dataFileBufferSize */
        MbToBytes(4) /* dataFileInitialSize */);
    ```

2. **Open RocksDB with Azure Storage**: 
    ```cpp
    using AVEVA::RocksDB::Plugin::Azure::Plugin;

    rocksdb::Options options;
    options.env = env; /* From step 1. */
   
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::Status status = rocksdb::DB::Open(options,
        Plugin::UniquePrefix("azureAccountURL", "blobContainerName"),
        &db);
   ```

3. **Use RocksDB Normally**: Once configured, use RocksDB APIs as you normally would - all data will be transparently stored in Azure Blob Storage.

###### Credential chain

`ChainedCredentialInfo` authenticates with the first source below that yields a token (each failure is logged at warning level):

1. **Service principal** (client id/secret/tenant from `ChainedCredentialInfo`), always tried first.
2. **Managed identity**, always tried: the user-assigned identity when a managed identity id is supplied, otherwise the system-assigned identity (IMDS or the `IDENTITY_ENDPOINT` of App Service/Functions/Container Apps).
3. **Environment**, only when `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` are all set.
4. **Workload identity**, only when `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_FEDERATED_TOKEN_FILE` are all set.

###### Building consumers on Windows

The plugin's public headers only forward-declare `boost::asio::io_context`, but the client libraries it links (`aveva::http-client`, `aveva::azure-client`) expose Boost.Asio, which must see the same `_WIN32_WINNT` in every translation unit. Those libraries therefore export `_WIN32_WINNT=0x0A00` (Windows 10) as a PUBLIC compile definition that reaches consumers through the plugin targets; consumers that set their own value must use the same one.

###### File cache

SST files can be cached on local disk. Pass `cachePath` (a directory on local storage) and `maxCacheSize` (bytes,
must be greater than zero for anything to be cached) to `Plugin::Register`; omit `cachePath` to disable the cache.
Only the primary storage account is cached; the backup never is.

```cpp
Plugin::Register(primaryDbOptions, &env, &guard, ioContext, storageCredentials, std::nullopt, logger,
                 MbToBytes(2),  /* dataFileBufferSize */
                 MbToBytes(4),  /* dataFileInitialSize */
                 "/var/cache/rocksdb-azure", /* cachePath */
                 size_t{20} * 1024 * 1024 * 1024 /* maxCacheSize: 20 GiB */);
```

An SST is downloaded only on its second access, least recently used files are evicted to stay within
`maxCacheSize`, and a file larger than `maxCacheSize` is never cached. See "File cache" in
[ARCHITECTURE.md](ARCHITECTURE.md) for details.

###### Configuration Options
- **Caching**: Enable local caching for frequently accessed data (see [File cache](#file-cache)). SSTs are cached on repeat access, which avoids eager one-time startup scans downloading every SST.
- **Bounded stale metadata**: Never-downloaded stale SST entries are capped and oldest metadata-only entries are pruned to prevent unbounded cache index growth.
- **Async IO**: The filesystem advertises `FSSupportedOps::kAsyncIO` and implements `ReadAsync`/`Poll`/`AbortIO`, so setting `rocksdb::ReadOptions::async_io = true` overlaps blob downloads with RocksDB's work (iterator readahead, `MultiGet`). Async reads are issued directly on the host `io_context` (no extra threads); completion handlers only copy bytes into RocksDB's buffer, and RocksDB's callbacks are delivered from `Poll`/`AbortIO` on the calling thread.

For detailed configuration examples and advanced usage patterns, see the plugin sources in [src/Azure](src/Azure) and the public headers in [include/AVEVA/RocksDB/Plugin](include/AVEVA/RocksDB/Plugin).

Plugins are compiled with RocksDB and can be optionally enabled at runtime. To learn more about RocksDB
plugins, please check out RocksDB's documentation on [building plugins](https://github.com/facebook/rocksdb/blob/main/plugin/README.md)
and the list of [known plugins](https://github.com/facebook/rocksdb/blob/main/PLUGINS.md) that are being developed. Below is the list of plugins that exist in this repository and are actively being developed and maintained by AVEVA.

## Breaking changes / migration (0.2.0)

The Azure client is now trimmed and in-tree, which changed the public surface:

* **`Core/BlobClient.hpp` and `Impl/PageBlob.hpp` are gone.** Code that included `AVEVA/RocksDB/Plugin/Core/BlobClient.hpp` should use `Core::ContainerClient` (`Core/ContainerClient.hpp`) for container-level operations, and the in-tree AzureClient types for blob I/O: `AVEVA::AzureClient::BlobContainerClient`, `PageBlobClient` and `BlobClient` (headers under `AVEVA/AzureClient/`, package `aveva-azure-client`, see [libs/README.md](libs/README.md)). Calls return `boost::asio::awaitable<std::expected<Response<T>, BlobStorageError>>` (coroutine-only, no completion token); co_await them from a coroutine on the client executor and never block on the host `io_context` thread.
* The Azure implementation classes (`BlobFilesystem` and the file, lock and directory types behind it) now take the injected `io_context` and client types. Consumers should not construct them directly: register the filesystem through `Plugin::Register` and let RocksDB create it from its `azblobfs...` URI.
* The old `Plugin::Register` overloads without an `io_context` remain as `[[deprecated]]` shims: they run the plugin on a process-wide, plugin-owned `io_context` thread. Migrate to the `io_context` overloads. The filesystem is registered under both the new `Plugin::NameFor` name and the legacy `Plugin::Name + db + backupDb` name so existing `azblobfs...` URIs keep resolving.
* `http:` endpoints are rejected unless `AllowInsecureHttp = true` is set on the client options (credentials are still refused over non-loopback `http`).
* Ranged downloads now require a valid `Content-Range` on every `206` response.
* `AzureClient::AzureRequestOptions::SetMaxRetries` and `HttpClientOptions::SetIdleConnectionTimeout` throw `std::invalid_argument` for negative values.
* `BlobFilesystem`'s private members moved behind a pimpl, so its size and layout changed (an ABI change; rebuild consumers). Its header no longer includes `RequestFailedException.hpp` or `LockFile.hpp`.
* `AzureClient` options and models trimmed to what the plugin uses: `ListBlobsOptions` keeps only `IncludeMetadata` of the `Include*` flags and lost `Delimiter`; `CreatePageBlobOptions::HttpHeaders` and `AccessTier`, `UploadPagesOptions::ContentMd5`, `UploadPagesFromUrlOptions::SourceAuthorization` and `GetPageRangesOptions::Range`/`MaxResults` were removed, as were `BlobHttpHeaders`, `AccessTier`, `BlobTags`, `ListBlobsResult::BlobPrefixes`/`Delimiter` and `BlobItem::Snapshot`/`Deleted`/`Tags`.
* Token endpoint failures now carry the HTTP status and OAuth error code (`TokenEndpointErrorCategory()`, `GetTokenEndpointFailure()` in `AVEVA/AzureClient/TokenEndpointError.hpp`) instead of mapping to `BlobStorageErrorCode::AuthenticationFailed`.
* `HttpClientOptions::SetDiagnosticsCallback()` (new, additive) reports stale-connection retries, connections that could not be pooled, swallowed exceptions, DNS cache invalidations and timeouts with the request phase. The plugin logs these through Boost.Log.
## Source and binary compatibility

**Compatibility with `main` is source-only. Binary (ABI) compatibility is not provided: rebuild every consumer
against this version.** No requirement for binary compatibility is recorded (the library already changes
`BlobFilesystem`'s layout, see above), and the executor injection cannot be hidden behind the old symbols, so the
exact old `Plugin::Register` signatures are not kept as separate entry points. The deprecated overloads gained a
trailing defaulted `maxPrefetchBytes` parameter, which changes their function type and mangled name.

Supported (checked by `tests/consumer`, built in-tree by `SourceCompatTests` and against the installed package by the
`install-consume` CI job):

* `Plugin::Register(configOptions, &env, &guard, primary, backup, logger, bufferSize, initialSize, cachePath,
  maxCacheSize)` for both `ServicePrincipalStorageInfo` and `ChainedCredentialInfo`, with or without the trailing
  arguments. It compiles with a deprecation warning and runs on a plugin-owned `io_context` thread. Migrate by
  inserting your `io_context` (or `std::shared_ptr<io_context>`) after `guard`.
* `Impl::Configuration::PageBlob::DefaultBufferSize` / `DefaultSize` (also available as `Defaults::*`).
* Normal RocksDB use of the registered filesystem through `rocksdb::FileSystem`
  (`NewWritableFile`, `NewRandomAccessFile`, `NewSequentialFile`, `NewRandomRWFile`).
* `ReadableFile`, `ReadWriteFile` and `WriteableFile` as `FSRandomAccessFile`, `FSRandomRWFile` and `FSWritableFile`.

Intentional breaks (no compatibility shim; use the replacement):

| Removed or changed | Replacement |
| --- | --- |
| `ReadableFile` was both `FSSequentialFile` and `FSRandomAccessFile` | `ReadableFile` is random access only; sequential reads use `SequentialFile` (obtain either through `rocksdb::FileSystem`) |
| Public constructors of `ReadableFile`, `ReadWriteFile`, `WriteableFile`, `SequentialFile` taking `Impl::*` objects | Not constructible by consumers; create them through `Plugin::Register` and RocksDB |
| `Impl/*` headers other than `Impl/Configuration.hpp` (`ReadableFileImpl.hpp`, `BlobFilesystemImpl.hpp`, `PageBlob.hpp`, ...) and `Core/BlobClient.hpp` | Not installed and unsupported; use the public API, `Core::ContainerClient`, or the `aveva-azure-client` package |
| Azure SDK types in public headers | The in-tree `AVEVA::AzureClient` library |

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

The Azure plugin talks to Blob Storage through the in-tree libraries in [libs/AzureClient](libs/AzureClient) and
[libs/HttpClient](libs/HttpClient), which are built as part of this project (Boost.Asio/Beast + OpenSSL from vcpkg).
TLS certificates are verified against the system trust store; on Windows the `ROOT` certificate store is exported
automatically. Set `SSL_CERT_FILE` or `SSL_CERT_DIR` to use a custom CA bundle instead.

## Dependency update automation

This repository uses [Dependabot](https://docs.github.com/en/code-security/dependabot) to automate dependency updates. Configuration lives in [`.github/dependabot.yml`](.github/dependabot.yml) and covers:

- **vcpkg** — bumps the `builtin-baseline` in [`vcpkg.json`](vcpkg.json).
- **GitHub Actions** — keeps workflow action versions current.

Dependency update PRs should only be merged after CI checks pass.

## Contributing

We are not accepting PRs from anyone outside of the AVEVA organization currently.
Please create an issue for proposed changes.
