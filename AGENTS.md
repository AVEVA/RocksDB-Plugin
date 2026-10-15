# RocksDB Plugin — Agent Instructions

The authoritative agent instructions for this repository are in
[.github/copilot-instructions.md](.github/copilot-instructions.md): scope, source layout (including `libs/` and
`.github/workflows/`), procedure, build and test commands, pull request workflow, conventions and known pitfalls.
Read that file first; this one intentionally does not duplicate it, so the two cannot drift apart.

Essentials:

- C++23, CMake and vcpkg. Configure with `cmake --preset <preset-name>` (see `CMakePresets.json`), build with
  `cmake --build build/<preset> --config Debug`, test with
  `ctest --test-dir build/<preset> --output-on-failure --build-config Debug`.
- vcpkg resolves dependencies from `vcpkg.json`; set `VCPKG_ROOT` before the first configure.
- Never call `.get()` on a thread that runs the host-supplied `io_context`.
- Map every Azure client error to a `rocksdb::Status`; never swallow errors.
- Never create or publish a pull request without explicit user approval.
