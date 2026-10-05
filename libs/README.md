# Vendored libraries

| Library | Path | Upstream repository | Vendored version |
|---------|------|---------------------|------------------|
| aveva-http-client | `libs/HttpClient` | TODO: record upstream URL | 0.0.1, imported in `af31859` |
| aveva-azure-client | `libs/AzureClient` | TODO: record upstream URL | 0.0.1, imported in `af31859` |

Both libraries are licensed under Apache-2.0 (see the `LICENSE` file in each directory and the
root `REUSE.toml`, which applies the SPDX identifier to every file under `libs/`).

The root `vcpkg.json` is the only manifest used by this repository's build; the `vcpkg.json` files
inside the libraries are kept for reference and standalone builds only.

## Updating

1. Copy the library's source tree from upstream over the matching directory, leaving out standalone-only
   files (CI pipelines, `CMakePresets.json`, `vcpkg-configuration.json`, `CHANGELOG.md`, backlog files).
2. Keep the Apache-2.0 `LICENSE` file.
3. Update the version column above, rebuild and run `ctest -E Integration`.
4. The plugin tests reuse `FakeHttpClient.hpp` and `TestFixtures.hpp` from `libs/AzureClient/tests` through the
   `aveva-azure-client-test-support` INTERFACE target (`infrastructure/cmake/AvevaClientLibraries.cmake`). Check
   that both headers still exist after a sync.
