# Onboarding — AVEVA RocksDB Plugin

Welcome. Read this document first when you join the RocksDB Plugin team. It answers
the questions that new joiners ask most often, so you can help yourself before you
ask a developer.

> If something is not clear, wrong, or missing, update this file in your first PR.
> Onboarding docs stay useful only when the person who hits the problem fixes it.

This document uses Simplified Technical English (ASD-STE100). Sentences are short.
Instructions use the imperative. Descriptions use the simple present tense.

---

## 1. What this repo is

The **AVEVA RocksDB Plugin** is a C++ library. It adds plugins to
[RocksDB](https://rocksdb.org/) that let RocksDB run **stateless** on top of
**Azure Blob Storage**.

The main plugin is the **Azure Page Blob Filesystem**. It makes an Azure blob
container look like a local filesystem to RocksDB. A service can then run in a
Kubernetes pod or a Service Fabric stateless service. The service does not need a
persistent local disk.

The repo also ships shared building blocks in `src/Core/`:

- A **local filesystem** abstraction (`LocalFilesystem`). Code uses it directly, and
  the caches use it as their backing store.
- A **file-based compressed secondary cache**
  (`FileBasedCompressedSecondaryCache`). RocksDB uses it as a secondary cache tier.
- A **file cache with an LRU index** (`FileCache`, `LruFileIndex`). It keeps hot
  blob content on the local disk.

See [`README.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/README.md) for the product summary. See
[`ARCHITECTURE.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/ARCHITECTURE.md) for the component map.

### Primary consumer: platform-graphdb-storage

The main consumer of this plugin is the **AVEVA `platform-graphdb-storage`**
service. It uses the plugin to store its RocksDB data in Azure Blob Storage instead
of on a local disk. This lets the service run as a stateless workload.

Keep this fact in mind when you change public headers or public behavior:

- A break in the public API breaks the `platform-graphdb-storage` build.
- A break in the on-disk or on-blob format can break live services.
- A change in an error code can change the recovery path in the consumer.

When you make a change that can be visible to the consumer, call it out in the PR
description. The PR template has a section for breaking changes. Fill it in.

---

## 2. Repository layout

| Path | What lives here |
|------|-----------------|
| `src/Azure/` | Azure Page Blob Filesystem. Blob I/O, directory ops, lock files, logger, and Azure SDK error translation. |
| `src/Azure/Impl/` | Internal parts of the Azure plugin. Page blob, container client, buffer chunking, log rate limiter, storage account, and configuration. |
| `src/Azure/Models/` | Value types for credentials and storage info (`ServicePrincipalStorageInfo`, `ChainedCredentialInfo`). |
| `src/Core/` | Shared filesystem, file cache, LRU index, secondary cache, RocksDB helpers, and utilities. |
| `include/AVEVA/RocksDB/Plugin/Azure/` | **Public** Azure plugin headers. |
| `include/AVEVA/RocksDB/Plugin/Core/` | **Public** Core headers. |
| `tests/AVEVA/RocksDB/Plugin/Azure/Impl/` | Azure plugin unit tests and integration tests. The integration tests need Azurite or a real Azure account. |
| `tests/AVEVA/RocksDB/Plugin/Core/` | Core unit tests, and mocks in `Mocks/` for `BlobClient`, `ContainerClient`, `File`, and `Filesystem`. |
| `infrastructure/cmake/` | CMake helper files. `CMakePresets.json` points `CMAKE_PREFIX_PATH` here. |
| `pipelines/` | Azure DevOps pipeline files. |
| `.github/` | GitHub config. PR template, issue templates, hooks, agent configs, and Copilot instructions. |

### Public vs. internal split

Follow these rules:

- Public headers live in `include/AVEVA/RocksDB/Plugin/`. Treat them as a
  backward-compatible contract. Downstream services (for example,
  `platform-graphdb-storage`) build against them.
- `Impl/` folders hold internal code. This applies to both
  `src/Azure/Impl/` and `include/AVEVA/RocksDB/Plugin/Azure/Impl/`. The `Impl/`
  headers are public only because the templated public headers need them at
  compile time. Do not treat them as a stable API.

---

## 3. First-day setup

### 3.1 Prerequisites

You need these tools:

1. Windows 10 or 11, or Linux. The CI covers both.
2. Git.
3. CMake 3.24 or later.
4. A C++23 compiler.
    - On Windows: MSVC in Visual Studio 2022 with the "Desktop development
      with C++" workload.
    - On Linux: a recent GCC or Clang. Look at the `LinuxDebug` and
      `LinuxRelease` flags in [`CMakePresets.json`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/CMakePresets.json). Your
      compiler must accept them without warnings.
5. [vcpkg](https://github.com/microsoft/vcpkg). The build resolves dependencies from
   [`vcpkg.json`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/vcpkg.json) with the manifest feature `testing`.
6. [clang-format and clang-tidy](https://releases.llvm.org/). The rules are in
   [`.clang-format`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.clang-format) and [`.clang-tidy`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.clang-tidy).
7. [gitleaks](https://github.com/gitleaks/gitleaks). The pre-commit hook blocks
   your commit if you do not install `gitleaks`.
8. On Windows only, turn on long path support. Set
   `HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled` to `1`.
   Without it, deep vcpkg and build paths fail.

### 3.2 Bootstrap vcpkg

Do these steps one time:

```powershell
git clone https://github.com/microsoft/vcpkg.git <vcpkg-path>
& <vcpkg-path>/bootstrap-vcpkg.bat        # On Linux, use bootstrap-vcpkg.sh
$env:VCPKG_ROOT = "<vcpkg-path>"          # Also add to System Environment Variables
```

Then restart your shell. The restart makes sure that later commands see
`VCPKG_ROOT`.

### 3.3 Run the environment doctor

Run this command from the repo root:

```powershell
./devdoctor.ps1
```

The doctor does these checks:

- It checks long path support, CMake, `clang-format`, `clang-tidy`, `gitleaks`,
  and `vcpkg`.
- It sets `core.hooksPath = .github/hooks` so the pre-commit hook runs on every
  commit.

Do not skip this step. If you skip it, you can commit unformatted code or leak
secrets. See [`.github/hooks/README.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.github/hooks/README.md) for more.

### 3.4 First build

Run these commands:

```powershell
# Configure. This creates build/<presetName>/.
cmake --preset WindowsDebug

# Build.
cmake --build build/WindowsDebug --config Debug

# Run tests.
ctest --test-dir build/WindowsDebug --output-on-failure --build-config Debug
```

The presets are in [`CMakePresets.json`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/CMakePresets.json). The names are
`WindowsDebug`, `WindowsRelease`, `LinuxDebug`, and `LinuxRelease`. The Release
presets use `RelWithDebInfo` and turn on interprocedural optimization.

> **CAUTION: Delete `build/<preset>/` and configure again when the build is
> unstable after a branch switch.** Do not try to fix the CMake cache by hand.
> A stale CMake cache is the most common wasted-hour on this repo. You can also
> add `--fresh` to `cmake --preset`.

---

## 4. Run the tests

The tests split into two groups. Both live under `tests/AVEVA/RocksDB/Plugin/`.

1. **Unit tests.** They use the mocks in `tests/.../Core/Mocks/`. Always run them.
2. **Integration tests.** The file names end with `IntegrationTests.cpp`. They
   talk to real Azure Blob Storage.
   You need credentials for these to pass.

Run all tests:

```powershell
ctest --test-dir build/WindowsDebug --output-on-failure --build-config Debug
```

Run one suite by name:

```powershell
ctest --test-dir build/WindowsDebug -R FileCache --output-on-failure --build-config Debug
```

If the integration tests fail but the unit tests pass, check your Azurite or
Azure credentials first. Do not assume that the code has a defect.

---

## 5. How to make a change

The expected loop is:

1. Open an issue that describes the bug or the change.
2. Classify the layer that you touch:
   - `src/Azure/` — Azure integration, credentials, error translation.
   - `src/Core/` — filesystem abstractions, caches, helpers.
   - `include/AVEVA/RocksDB/Plugin/` — public API (see §2 for stability).
3. Write or update tests. All functional changes need tests. Prefer unit tests
   with the existing mocks. Add an integration test only when the behavior needs
   a real backend to observe.
4. Build and run the tests for your preset (see §3.4).
5. Commit. The pre-commit hook runs `gitleaks` and `clang-format` on the staged
   files under `src/`, `include/`, and `tests/`.
6. Open a PR. Follow the [PR template](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.github/PULL_REQUEST_TEMPLATE.md). The
   title format is `<type>(<scope>): <subject>`. Use `feat`, `fix`, `docs`,
   `refactor`, `perf`, `test`, `build`, `chore`, `ci`, or `revert`. For a
   breaking change to a public header, use `feat(scope)!: ...`.
7. Fill in the security compliance checklist in the PR template. Reviewers
   check the items.

### Code conventions

Follow these rules:

- Use C++23. All presets treat warnings as errors.
- Use modern C++ idioms: RAII, smart pointers, and clear ownership. Do not use
  raw `new` and `delete`.
- Translate every Azure SDK failure to a meaningful `rocksdb::Status`. Do not
  swallow errors. Use `AzureErrorTranslator` for the mapping. Do not map codes
  by hand at the call site.
- Make cache and filesystem components thread-safe. When you add state,
  document the invariants and cover them with tests.
- Do not leak implementation details into public headers. Do not include Azure
  SDK types in `include/AVEVA/RocksDB/Plugin/`, and add only the Boost types
  that we already use.
- Add new source files to the nearest `CMakeLists.txt`.
- Write comments about *why*, not *what*. Add a short comment block for
  functions that are more than about 5 lines.
- Follow [`.clang-format`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.clang-format) and [`.clang-tidy`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.clang-tidy).
  The hook formats the staged files for you, but you must fix the tidy
  warnings.

---

## 6. Use the plugin — examples

You do not need to ship a service that uses this plugin to review code. But you
must know the shape of the API. The examples below use the real signatures from
`include/AVEVA/RocksDB/Plugin/Azure/Plugin.hpp`.

### 6.1 The end-to-end flow

The flow is:

1. The consumer builds a credentials object. It is either
   `ServicePrincipalStorageInfo` or `ChainedCredentialInfo`.
2. The consumer calls `Plugin::Register(...)`. The call gives back a
   `rocksdb::Env*`. The Azure Page Blob Filesystem powers this env.
3. The consumer sets `options.env` to that pointer. The consumer opens the
   database at the path that `StorageAccount::UniquePrefix(...)` returns.
4. From here, the consumer calls RocksDB in the normal way. Every file
   operation goes through `BlobFilesystem` → `BlobFilesystemImpl` → the Azure
   SDK page blob calls.
5. The local caches (`FileCache` and `FileBasedCompressedSecondaryCache`)
   sit between hot reads and the blob backend. They stop repeated downloads
   of the same SST.

### 6.2 Register with a service principal (dev and test)

Use a service principal for local development and for tests. Do not use it in
production.

```cpp
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp>

#include <rocksdb/db.h>
#include <boost/log/sources/severity_logger.hpp>

#include <memory>

using AVEVA::RocksDB::Plugin::Azure::Plugin;
using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;
using AVEVA::RocksDB::Plugin::Azure::Impl::StorageAccount;

constexpr int64_t kMib = 1024LL * 1024LL;

ServicePrincipalStorageInfo primaryCreds{
    "myContainer",                 // dbName — the blob container name
    "https://myacct.blob.core.windows.net",
    "<service-principal-id>",
    "<service-principal-secret>",
    "<tenant-id>"
};

rocksdb::ConfigOptions configOptions;
rocksdb::Env* env = nullptr;
std::shared_ptr<rocksdb::Env> envGuard;

auto logger = std::make_shared<
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

rocksdb::Status status = Plugin::Register(
    configOptions,
    &env,
    &envGuard,
    primaryCreds,
    std::nullopt,   // No backup account.
    logger,
    2 * kMib,       // dataFileBufferSize:  2 MiB.
    4 * kMib);      // dataFileInitialSize: 4 MiB.

if (!status.ok()) { /* handle */ }

rocksdb::Options options;
options.env = env;
options.create_if_missing = true;

rocksdb::DB* rawDb = nullptr;
status = rocksdb::DB::Open(
    options,
    StorageAccount::UniquePrefix(
        "https://myacct.blob.core.windows.net",
        "myContainer"),
    &rawDb);

std::unique_ptr<rocksdb::DB> db{rawDb};
```

### 6.3 Register with a chained credential (production)

Use `ChainedCredentialInfo` in production. It tries managed identity first, then
falls back to the service principal. This is the path that
`platform-graphdb-storage` uses when it runs in Azure.

```cpp
#include <AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp>

using AVEVA::RocksDB::Plugin::Azure::Models::ChainedCredentialInfo;

ChainedCredentialInfo primaryCreds{
    "myContainer",
    "https://myacct.blob.core.windows.net",
    "<service-principal-id>",
    "<service-principal-secret>",
    "<tenant-id>",
    std::optional<std::string>{"<user-assigned-mi-client-id>"}
};

// Turn on the local file cache with a 4 GiB cap.
std::optional<std::string_view> cachePath = "C:/data/rocksdb-cache";
size_t maxCacheSize = 4ULL * 1024ULL * kMib;

rocksdb::Status status = Plugin::Register(
    configOptions,
    &env,
    &envGuard,
    primaryCreds,
    std::nullopt,
    logger,
    2 * kMib,
    4 * kMib,
    cachePath,
    maxCacheSize);
```

### 6.4 Add the compressed secondary cache

The consumer can plug in the `FileBasedCompressedSecondaryCache` as a
secondary tier of the RocksDB block cache. The cache writes RocksDB block
cache spill to disk, and it applies LRU eviction.

```cpp
#include <AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp>
#include <AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>

using AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache;
using AVEVA::RocksDB::Plugin::Core::LocalFilesystem;

auto fs = std::make_shared<LocalFilesystem>(/* args as needed */);
auto secondary = std::make_shared<FileBasedCompressedSecondaryCache>(
    std::filesystem::path{"C:/data/rocksdb-l2"},
    fs,
    512ULL * kMib,   // 512 MiB capacity.
    logger);

rocksdb::LRUCacheOptions primaryCacheOpts;
primaryCacheOpts.capacity = 256ULL * kMib;
primaryCacheOpts.secondary_cache = secondary;
auto blockCache = rocksdb::NewLRUCache(primaryCacheOpts);

rocksdb::BlockBasedTableOptions tableOpts;
tableOpts.block_cache = blockCache;
options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOpts));
```

### 6.5 How `platform-graphdb-storage` uses this

The `platform-graphdb-storage` service integrates the plugin in the same
pattern as above. The key points are:

- The service creates a `ChainedCredentialInfo` from its own configuration
  (managed identity in Azure, service principal in local dev).
- The service calls `Plugin::Register(...)` one time at start-up. It stores
  the returned `rocksdb::Env*` for the life of the process.
- The service opens each RocksDB instance at
  `StorageAccount::UniquePrefix(accountUrl, container)`.
- The service turns on the local file cache and the secondary cache to keep
  the read path fast against blob storage.

When you change how `Plugin::Register` behaves, how the env maps paths, or how
errors translate, check the change against this consumer path. If you cannot
build against the consumer, at least add or update a test in
`tests/AVEVA/RocksDB/Plugin/Azure/Impl/` that covers the scenario.

---

## 7. Known pitfalls — read this before you debug

- **The CMake cache is stale after a branch switch.** Delete `build/<preset>/`
  and configure again. Do not try to fix the cache.
- **Long path support on Windows.** vcpkg and build paths go very deep. Turn
  on long path support system-wide. `devdoctor.ps1` warns you when it is off.
- **The Azure SDK is asynchronous.** Every call that returns a future needs a
  correct `.get()` or `.wait()`. A missing wait is the source of most flakes.
- **Error translation is easy to miss.** When you add a new Azure SDK call,
  extend `AzureErrorTranslator` and add a test for the new code path.
- **`Impl/` headers are public but not stable.** Do not build downstream code
  against them for the long term. The stable contract is the non-`Impl/`
  headers.
- **Integration tests need a backend.** A failed integration test is usually
  a configuration problem: Azurite is not running, or the credentials are not
  valid. Check that first, before you change code.
- **`VCPKG_MANIFEST_FEATURES=testing`.** The presets set this so the test
  dependencies come in. If you configure with a hand-written command line and
  skip the presets, set the variable, or the tests do not link.

---

## 8. Useful pointers

Docs in this repo:

- [`README.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/README.md) — product summary and a first usage example.
- [`ARCHITECTURE.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/ARCHITECTURE.md) — component map.
- [`CONTRIBUTING.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/CONTRIBUTING.md) — day-to-day flow.
- [`SECURITY.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/SECURITY.md) — how to report a security issue.
- [`AGENTS.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/AGENTS.md) and
  [`.github/copilot-instructions.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.github/copilot-instructions.md) — how
  AI agents behave in this repo. This is useful context when you use Copilot
  or another agent.
- [`.github/PULL_REQUEST_TEMPLATE.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.github/PULL_REQUEST_TEMPLATE.md) — PR
  expectations and the security checklist.
- [`.github/hooks/README.md`](https://github.com/AVEVA/RocksDB-Plugin/blob/main/.github/hooks/README.md) — pre-commit hook
  details.

External docs:

- [RocksDB documentation](https://rocksdb.org/) and the
  [plugin guide](https://github.com/facebook/rocksdb/blob/main/plugin/README.md).
- [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp).
- [Azure Blob Storage — page blobs](https://learn.microsoft.com/azure/storage/blobs/storage-blob-pageblob-overview).
- [Azurite (local emulator)](https://learn.microsoft.com/azure/storage/common/storage-use-azurite).

---

## 9. Who to ask — last resort

Read this document, the linked docs, and the closed PRs and issues first. If
you cannot make progress after that:

1. Send a message in the team channel with a link to what you tried.
