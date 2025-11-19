# AVEVA RocksDB Plugins

This project contains AVEVA's plugins for the [RocksDB](https://rocksdb.org/) database. Plugins
are compiled with RocksDB and can be optionally enabled at runtime. To learn more about RocksDB
plugins, please check out RocksDB's documentation on [building plugins](https://github.com/facebook/rocksdb/blob/main/plugin/README.md)
and the list of [known plugins](https://github.com/facebook/rocksdb/blob/main/PLUGINS.md) that are being developed. Below is the list
of plugins that exist in this repository and are actively being developed and maintained by AVEVA.

## Plugin List

### [Azure Page Blob Filesystem](src/AVEVA/RocksDB/Plugin/Azure)

This plugin uses the [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp) for interacting with page blobs
in place of a local filesystem. This allows a service using RocksDB to be deployed in a "stateless" manner
(e.g., Kubernetes Pod, Service Fabric stateless service, etc.) and connect to an Azure blob container
for all of its storage needs. This is similar to using
[SMB network shares](https://learn.microsoft.com/en-us/windows-server/storage/file-server/file-server-smb-overview)
or a FUSE filesystem (if deploying on Linux where [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) is supported).
However, a fully integrated solution allows for purpose tuned performance for RocksDB, self-contained deployments (as opposed to having to configure SMB shares or FUSE on the host system), and a whole host of other useful things.

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

## Contributing

We are not accepting PRs from anyone outside of the AVEVA organization currently.
Please create an issue for proposed changes.
