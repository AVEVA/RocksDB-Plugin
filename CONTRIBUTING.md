# Contributing

This repository is currently maintained by AVEVA teams.

## Before You Start

1. Open an issue describing the bug or proposed change.
2. Align on scope before opening a pull request.
3. Ensure your local environment is configured with `VCPKG_ROOT`.
4. Run `./devdoctor.ps1` from the repository root. It validates required tooling
   (CMake, clang-format/clang-tidy, gitleaks, vcpkg) and configures the shared git
   hooks path (`core.hooksPath = .github/hooks`) so the pre-commit hook runs locally.

## Build and Test

1. Configure: `cmake --preset WindowsDebug` or `cmake --preset LinuxDebug`
2. Build: `cmake --build build/<preset> --config Debug`
3. Test: `ctest --test-dir build/<preset> --output-on-failure --build-config Debug`

## Code Guidelines

1. Follow modern C++ practices (RAII, strong typing, clear ownership).
2. Keep public headers in `include/AVEVA/RocksDB/Plugin/` backward compatible.
3. For Azure layer changes, preserve complete Azure SDK error to RocksDB status mapping.
4. Add tests for all functional changes.

## Pull Request Expectations

1. Keep PRs focused and small where possible.
2. Include a clear summary, risk assessment, and verification steps.
3. Update docs when behavior or configuration changes.
