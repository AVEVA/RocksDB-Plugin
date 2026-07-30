# RocksDB Plugin — Agent Instructions

## Scope
This is the **AVEVA RocksDB Plugin** — a C++ library providing AVEVA-specific plugins for RocksDB, enabling cloud-native stateless deployment via Azure Blob Storage. The primary abstraction is an Azure Page Blob Filesystem that allows RocksDB to treat Azure Blob Storage as a local filesystem.

**Primary stack:** C++ (CMake)

### Key source layout
| Path | Purpose |
|------|---------|
| `src/Azure/` | Azure Page Blob Filesystem plugin (blob storage integration, file read/write, directory ops, error translation) |
| `src/Core/` | Filesystem abstractions: local filesystem, compressed secondary cache, file cache with LRU index, utilities |
| `include/AVEVA/RocksDB/Plugin/` | Public C++ headers |
| `tests/` | C++ test suites |
| `infrastructure/` | Build and CI infrastructure |

## Procedure
1. Classify whether the change is in the Azure storage layer (`src/Azure/`) or the Core abstractions (`src/Core/`).
2. For Azure layer changes, verify error translation is complete — map all Azure SDK error codes to appropriate RocksDB `Status` codes.
3. For file cache changes (`Core/FileCache`), confirm LRU eviction logic and thread safety.
4. Rebuild and run tests after any native change.
5. Check that public headers in `include/` remain backward compatible.

## Build and Test
```powershell
# Configure (from repo root, creates build/ directory)
cmake --preset <preset-name>

# Build
cmake --build build/

# Run tests
ctest --test-dir build/ --output-on-failure
```

> Check `CMakePresets.json` for available presets. Delete the `build/` directory and reconfigure if the build behaves inconsistently after a branch switch.

## Conventions
- Follow modern C++ (C++17 or newer) idioms.
- Use RAII for resource management — avoid raw pointers with manual `delete`.
- Map Azure SDK errors to meaningful RocksDB `rocksdb::Status` codes; do not swallow errors silently.
- New source files must be added to the owning `CMakeLists.txt`.
- Public headers go in `include/AVEVA/RocksDB/Plugin/` — do not leak implementation details into headers.

## Known Pitfalls
- CMake cache issues are common after branch switches — delete `build/` and reconfigure rather than patching the cache.
- On Windows, long path support must be enabled for stable builds.
- Azure SDK calls are asynchronous; ensure proper `.get()` / `.wait()` handling on futures to avoid races.
- `vcpkg` is used for dependency management — run `vcpkg install` from `vcpkg-configuration.json` before first build.
