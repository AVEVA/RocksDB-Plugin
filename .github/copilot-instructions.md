---
applyTo: "**"
---

# RocksDB Plugin — Agent Instructions

## Scope

This is the **AVEVA RocksDB Plugin** — a C++ library providing AVEVA-specific plugins for
[RocksDB](https://rocksdb.org/), enabling cloud-native stateless deployment via Azure Blob
Storage. The primary abstraction is an Azure Page Blob Filesystem that lets RocksDB treat
Azure Blob Storage as a local filesystem.

**Primary stack:** C++ (CMake + vcpkg).

### Source layout

| Path | Purpose |
|------|---------|
| `src/AVEVA/RocksDB/Plugin/Azure/` | Azure Page Blob Filesystem plugin (blob storage integration, file read/write, directory ops, error translation) |
| `src/AVEVA/RocksDB/Plugin/Core/` | Filesystem abstractions: local filesystem, compressed secondary cache, file cache with LRU index, utilities |
| `include/AVEVA/RocksDB/Plugin/` | Public C++ headers |
| `tests/` | C++ test suites |
| `infrastructure/` | Build and CI infrastructure |
| `pipelines/` | Azure DevOps pipeline definitions |

## Procedure

1. Classify the change: Azure storage layer (`src/.../Azure/`) or Core abstractions (`src/.../Core/`).
2. For Azure-layer changes, verify error translation is complete — map all Azure SDK error
   codes to appropriate RocksDB `rocksdb::Status` codes. Never swallow errors silently.
3. For file-cache changes (`Core/FileCache`), confirm LRU eviction logic and thread safety.
4. Plan the smallest safe change that preserves the public header API surface.
5. Implement the change, adding useful logging around failure-prone or stateful operations.
6. Rebuild and run tests after any native change (see Build and Test).
7. Verify public headers in `include/` remain backward compatible.
8. Provide a change summary: files changed, tests added or updated, test run summary.
9. For PR creation, follow the Pull Request Workflow section.

## Build and Test

```powershell
# Configure (creates the build/ directory; check CMakePresets.json for available presets)
cmake --preset <preset-name>

# Build
cmake --build build/<preset> --config Debug

# Run tests
ctest --test-dir build/<preset> --output-on-failure --build-config Debug
```

> Delete the `build/` directory and reconfigure (or use `--fresh`) if the build behaves
> inconsistently after a branch switch. `vcpkg` resolves dependencies from `vcpkg.json`;
> ensure `VCPKG_ROOT` is set before the first configure.

Run `./devdoctor.ps1` from the repository root to validate the environment (long path
support, CMake, clang-format/clang-tidy, gitleaks, vcpkg, and git hook configuration).

## Pull Request Workflow

> **Hard rule: NEVER create or publish a PR without explicit user approval.**

1. Read [.github/PULL_REQUEST_TEMPLATE.md](PULL_REQUEST_TEMPLATE.md)
   **before** composing the PR description.
2. Populate all template sections: PR type, current/new behavior, how it was verified, and the
   security compliance checklist.
3. **Stop and ask the user to confirm before creating the PR.** Show the draft description and
   wait for explicit approval.
4. After the PR is created, reply to any addressed review comments confirming the fix was
   committed and pushed.

## Conventions

- Follow modern C++ (C++17 or newer) idioms.
- Use RAII for resource management — avoid raw pointers with manual `delete`.
- Map Azure SDK errors to meaningful `rocksdb::Status` codes; do not swallow errors silently.
- New source files must be added to the owning `CMakeLists.txt`.
- Public headers go in `include/AVEVA/RocksDB/Plugin/` — do not leak implementation details
  into headers.
- Respect [.clang-format](../.clang-format) and [.clang-tidy](../.clang-tidy); the pre-commit
  hook runs `clang-format` on staged C/C++ files under `src/`, `include/`, and `tests/`.

## Documentation/Comments

- Document functions longer than ~5 lines with a brief comment block.
- Comments are for humans. Focus on the *why*, not the *what*.

## Known Pitfalls

- CMake cache issues are common after branch switches — delete `build/` and reconfigure
  rather than patching the cache.
- On Windows, long path support must be enabled for stable builds (`devdoctor.ps1` reports this).
- Azure SDK calls are asynchronous; ensure proper `.get()` / `.wait()` handling on futures to
  avoid races.
- `vcpkg` is used for dependency management — ensure `VCPKG_ROOT` is set and bootstrap vcpkg
  before the first build.
