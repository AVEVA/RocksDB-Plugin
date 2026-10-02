# Contributing

## Build locally

Use the checked-in CMake presets:

```powershell
cmake --preset WindowsDebug
cmake --build --preset WindowsDebug
ctest --preset WindowsDebug
```

Linux builds use the corresponding `LinuxDebug` / `LinuxRelease` presets. Sanitizer presets are available for `WindowsDebugASan`, `LinuxDebugASan`, and `LinuxDebugUBSan`.

The default presets enable the manifest `tests` feature automatically. For custom configure commands, enable it yourself with `VCPKG_MANIFEST_FEATURES=tests` or `vcpkg install --x-feature=tests`. Benchmarks additionally require the `benchmarks` feature.

Warnings are errors in the presets (`AVEVA_AZURE_CLIENT_WARNINGS_AS_ERRORS=ON`, i.e. `/WX` or `-Werror`); non-preset builds default to `OFF`. Build options use the `AVEVA_AZURE_CLIENT_` prefix (`..._ENABLE_ASAN`, `..._ENABLE_UBSAN`, `..._ENABLE_TSAN`, `..._ENABLE_COVERAGE`, `..._ENABLE_CLANG_TIDY`, `..._RUN_INTEGRATION_TESTS`); the old unprefixed names still work for one release but print a deprecation warning.

## Style and static analysis

- Format changes with the repository `.clang-format` using the pinned **clang-format 22** (CI fails on any difference: `git ls-files '*.cpp' '*.hpp' | xargs clang-format-22 --dry-run --Werror`). The bulk reformat commit is listed in `.git-blame-ignore-revs`; use `git config blame.ignoreRevsFile .git-blame-ignore-revs` locally.
- Run `clang-tidy` with the checked-in `.clang-tidy` profile when touching production code
- Build the public API on Asio `async_initiate` so every operation accepts any completion token (callback, `boost::asio::use_future`, `deferred`, coroutines); do not add operations that hard-code a single token type
- Keep source comments to the why in at most two lines; migration notes, task references, and extended rationale belong in docs, commit messages, or issues

## Tests

- Add or update unit tests in `tests/` for every behavior change
- Keep integration-only coverage in `aveva-azure-client-integration-tests`; it is labeled `integration` in CTest
- Optional benchmarks belong in `benchmarks/`

Coverage options:

Coverage measures library code only (`src/` and `include/`); test sources are excluded so they cannot inflate the
number. `--merge-lines` counts each source line once rather than once per template instantiation (the
completion-token machinery is instantiated for every operation x token type). The CI gate (thresholds in
`pipelines/templates/shared-variables.yml`, currently 90% lines / 80% branches) only ever goes up.

- Linux (what CI runs; coverage is GCC-only, Clang is rejected at configure time; the unit-only report is the sole coverage gate, there is no combined unit+integration report because integration tests need Azure credentials): configure with `-DAVEVA_AZURE_CLIENT_ENABLE_COVERAGE=ON`, run `ctest -L unit`, then
  `gcovr --root . --filter src --filter include --exclude-throw-branches --exclude-unreachable-branches --branches --merge-lines --gcov-ignore-parse-errors=negative_hits.warn --print-summary`
- Windows: `OpenCppCoverage --sources src --sources include --cover_children --export_type cobertura:coverage.xml -- ctest --preset WindowsDebug -L unit`

Benchmarks (configure with `-DAVEVA_AZURE_CLIENT_BENCHMARKS=ON` and the `benchmarks` vcpkg feature): `ctest -L benchmark` runs a fast smoke pass (`--benchmark_min_time=0.01s`). For real measurements run the binary directly, e.g. `aveva-azure-client-benchmarks --benchmark_min_time=1s --benchmark_repetitions=5 --benchmark_report_aggregates_only=true`, on a Release build.

Installed-package consumer test: `ctest -L install` (after a full build) installs the library to a scratch prefix, then configures, builds and runs `tests/cmake/consumer` through `find_package(aveva-azure-client CONFIG REQUIRED)`. Disable it with `-DAVEVA_AZURE_CLIENT_INSTALL_TEST=OFF`.

Parser fuzzing:

The response parsers (XML, dates, `Content-Range`, error bodies) have a libFuzzer harness in
`tests/fuzz/ParseXmlFuzz.cpp`. The first input byte selects the parser; any exception that is not a `std::exception`, or
any sanitizer report, is a bug. The unit tests replay seed inputs through the same harness on every build. To fuzz for
real you need Clang with the GNU-style driver:

```bash
cmake -B build/fuzz -DCMAKE_CXX_COMPILER=clang++ -DAVEVA_AZURE_CLIENT_FUZZ=ON -DAVEVA_AZURE_CLIENT_TESTS=OFF
cmake --build build/fuzz --target aveva-azure-client-parse-fuzz
./build/fuzz/tests/fuzz/aveva-azure-client-parse-fuzz -max_total_time=300 -max_len=65536 corpus/
```

## Logging and secrets

The library does not log today. If logging is ever added, pass headers, URLs and form bodies through `src/Redaction.hpp` (`RedactHeaderForDiagnostics`, `RedactUrlForDiagnostics`, `RedactFormBodyForDiagnostics`) first so `Authorization`, the SAS `sig` parameter and token-endpoint secrets never reach a log.

## Versioning

The project follows [semantic versioning](https://semver.org). While the major version is 0 the API may still change between minor releases; from 1.0 onward breaking API changes require a major bump. No ABI stability is promised: the library is built from source (or the vcpkg port) alongside the consumer, so rebuild after upgrading. `project(... VERSION ...)` in `CMakeLists.txt`, `vcpkg.json`, `infrastructure/overlay-ports/aveva-azure-client/vcpkg.json` and the `CHANGELOG.md` heading must be updated together; the `VersionConsistency` CTest test (`ctest -L version`) fails if the first three differ.

## Pull requests

- Keep changes focused and scoped to the problem being solved
- Update docs/CMake/tests alongside behavior or packaging changes
- When adding a public `...Async` operation, add a row to `tests/PublicApiCoverage.md` and fill the cells (or leave an explicit `TODO`)
- Ensure the default preset still configures, builds, and passes tests before requesting review
