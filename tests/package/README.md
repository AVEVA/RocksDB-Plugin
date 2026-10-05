# Package consumer smoke test

Verifies that the installed packages resolve (`find_package`), carry every transitive `find_dependency`, and link.
It is not part of the main build because it needs an install tree.

```powershell
cmake --preset WindowsDebug
cmake --build build/WindowsDebug --config Debug
cmake --install build/WindowsDebug --config Debug --prefix build/install
cmake -S tests/package -B build/package-test `
  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" `
  -DVCPKG_MANIFEST_MODE=OFF -DCMAKE_CONFIGURATION_TYPES=Debug `
  -DCMAKE_PREFIX_PATH="$PWD/build/install;$PWD/build/WindowsDebug/vcpkg_installed/x64-windows"
cmake --build build/package-test --config Debug
```

Adjust the vcpkg triplet directory to the one the preset uses. To run the executable, put the vcpkg\n`bin` and `debug/bin` directories of that triplet on `PATH`; it prints `ok`.
