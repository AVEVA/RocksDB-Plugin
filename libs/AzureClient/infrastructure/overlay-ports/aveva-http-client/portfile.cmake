vcpkg_from_git(
    OUT_SOURCE_PATH SOURCE_PATH
    URL "C:/Users/nathaniel.wright/OneDrive - AVEVA Solutions Limited/Projects/HttpClient"
    REF d3dc9fef80b1b630e345aa0905fa1e444687f3d1
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DAVEVA_HTTP_CLIENT_TESTS=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME aveva-http-client)
vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

# No LICENSE file in the source repo yet; record a placeholder copyright.
file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/copyright" "Proprietary - AVEVA Solutions Limited. Internal use only.\n")
