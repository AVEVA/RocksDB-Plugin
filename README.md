# AVEVA RocksDB Plugins

## Plugin List

### [Azure Page Blob Filesystem](src/AVEVA/RocksDB/Plugin/Azure)

* Uses the [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp) for interacting with page blobs in place of a local fileystem.

## Building

1. Ensure that vcpkg is installed and in the PATH
    * `git clone https://github.com/microsoft/vcpkg.git <vcpkg-path>`
    * `<vcpkg-path>/booststrap-vcpkg.[bat/sh]`
    * Set environment variable `VCPKG_ROOT` to `<vcpkg-path>`
    * Add `VCPKG_ROOT` to `PATH` environment variable
    * Reload shell to update environment variables
2. `git clone https://github.com/AVEVA/RocksDB-Plugin.git <rocksdb-plugin-path>`
3. `cmake -S <rocksdb-plugin-path> -B <rocksdb-plugin-build-path> --preset [Windows/Linux][Debug/Release]`
    * Presets can be found in [CMakePresets.json](CMakePresets.json)
4. `cmake --build <rocksdb-plugin-build-path>`
