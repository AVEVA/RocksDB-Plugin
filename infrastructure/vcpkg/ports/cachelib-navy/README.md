# cachelib-navy overlay port

Builds the flash engine (Navy) of [CacheLib](https://github.com/facebook/CacheLib) as a single static
library, `cachelib-navy::cachelib_navy`, for `CacheLibSecondaryCache`. Linux x64 only.

Upstream CacheLib is built with getdeps and requires fbthrift, fizz, wangle, mvfst, gtest, libnuma and
others. This port builds only what Navy needs, with its own [CMakeLists.txt](CMakeLists.txt), so the only
new vcpkg dependencies are folly, magic-enum, tsl-sparse-map, and xxhash.

Navy is built with `CACHELIB_IOURING_DISABLE` and uses its synchronous I/O path (pread/pwrite on reader and
writer thread pools). The vcpkg folly port never defines `FOLLY_HAS_LIBURING`/`FOLLY_HAS_LIBAIO` (its
`fix-deps.patch` reads `VCPKG_LOCK_FIND_LibUring` instead of `VCPKG_LOCK_FIND_PACKAGE_LibUring`, and
libaio is unset too), so folly's `IoUring`/`AsyncIO` classes are unavailable. Enabling io_uring later
requires a folly overlay port or an upstream vcpkg fix, plus removing that define.

## What is changed from upstream

| Item | Why |
|---|---|
| `CMakeLists.txt` (port owned) | Compiles `cachelib/navy/**` minus tests/benchmarks/`MockDevice`, plus the few `common/`, `shm/` and `allocator/nvmcache/Navy*` sources Navy references. |
| `shim/` headers | Replace the fbthrift generated types (`objects_types.h`, `BloomFilter_types.h`) with plain structs and the thrift serializers with stubs that throw. Navy only serializes them to persist the cache across restarts, which this integration never does (the cache file is truncated on open and `Driver::persist()` is never called). |
| `0001-remove-test-only-dependencies.patch` | `FRIEND_TEST` comes from `cachelib/common/FriendTest.h` instead of gtest; `NavySetup.cpp` rejects `BadDeviceStatus` test configs instead of building a gmock `MockDevice`. |
| `0002-header-only-numa.patch` | libnuma (LGPL-2.1) is replaced by `cachelib/shm/NumaShim.h`, which implements the handful of calls used by the shared-memory helpers through raw `mbind`/`set_mempolicy`/`get_mempolicy` syscalls. Navy never binds memory to a NUMA node. |
| `0003-build-fixes.patch` | Builds against the vcpkg folly/fmt: `format_as` for `CombinedEntryStatus` (fmt 12 does not format enums implicitly), no folly exception tracer in `printExceptionStackTraces`, and libaio code paths guarded by `FOLLY_HAS_LIBAIO`. |

The CacheLib DRAM tier (`CacheAllocator`) is not built.

## Licensing

CacheLib is Apache-2.0. New transitive dependencies: folly (Apache-2.0), magic_enum (MIT),
tsl-sparse-map (MIT), xxHash (BSD-2-Clause). No LGPL/GPL code is linked.

## Updating CacheLib

1. Change `version-string` in [vcpkg.json](vcpkg.json) to the new CacheLib release tag (without `v`)
   and set `SHA512` in [portfile.cmake](portfile.cmake) to `0`; the first build prints the real hash.
2. Re-apply the patches (`git apply --3way`) and regenerate them if they drift.
3. Diff `cachelib/navy/CMakeLists.txt` upstream against the source list in [CMakeLists.txt](CMakeLists.txt).
4. Diff `cachelib/navy/serialization/objects.thrift` and `cachelib/common/BloomFilter.thrift` against the
   shim structs; add any new fields or structs.
5. `grep -rn 'gtest\|gmock\|numa.h\|thrift/' cachelib/navy cachelib/common cachelib/shm` for new includes.
6. Run the full plugin test suite on Linux (`ctest --preset LinuxDebug`).

A vcpkg `builtin-baseline` bump (Dependabot) can change folly; the same test run covers that.
