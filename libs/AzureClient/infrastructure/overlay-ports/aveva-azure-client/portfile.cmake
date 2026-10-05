get_filename_component(SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DAVEVA_AZURE_CLIENT_TESTS=OFF
        -DAVEVA_AZURE_CLIENT_BENCHMARKS=OFF
        -DAVEVA_AZURE_CLIENT_EXAMPLES=OFF
        -DAVEVA_AZURE_CLIENT_INSTALL_TEST=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME aveva-azure-client)
vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
