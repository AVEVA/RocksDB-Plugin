# Newer GoogleTest deprecates testing::Invoke; the plugin tests that use it are rewritten when the plugin moves to the
# in-tree clients, so keep the deprecation from failing the build until then.
if(MSVC)
    add_compile_options(/wd4996)
else()
    add_compile_options(-Wno-deprecated-declarations)
endif()
find_package(GTest CONFIG REQUIRED)
include_directories(.)
add_subdirectory(AVEVA/RocksDB/Plugin/Core)
add_subdirectory(AVEVA/RocksDB/Plugin/Azure/Impl)
