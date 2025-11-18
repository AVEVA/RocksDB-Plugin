# AVEVA RocksDB Plugins

## Plugin List

### [Azure Page Blob Filesystem](src/AVEVA/RocksDB/Plugin/Azure)

* Uses the [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp) for interacting with page blobs in place of a local fileystem.

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
