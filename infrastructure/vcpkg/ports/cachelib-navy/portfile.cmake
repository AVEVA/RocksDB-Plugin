vcpkg_check_linkage(ONLY_STATIC_LIBRARY)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO facebook/CacheLib
    REF "v${VERSION}"
    SHA512 cb5b8b00ffc0fb3ee743416125875b25c62b9620e6dc6657bb3be0c93ae3e5be1f9c30577cac8db57518ccddf63b03a5980cd7626c3bb0059f48c3db1fd72345
    HEAD_REF main
    PATCHES
        0001-remove-test-only-dependencies.patch
        0002-header-only-numa.patch
        0003-build-fixes.patch
)

# Port-owned build: only the Navy subset, thrift replaced by shim headers.
file(COPY
    "${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt"
    "${CMAKE_CURRENT_LIST_DIR}/cachelib-navy-config.cmake.in"
    DESTINATION "${SOURCE_PATH}/aveva-navy")
file(COPY "${CMAKE_CURRENT_LIST_DIR}/shim" DESTINATION "${SOURCE_PATH}/aveva-navy")

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}/aveva-navy"
)
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(CONFIG_PATH share/cachelib-navy)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include" "${CURRENT_PACKAGES_DIR}/debug/share")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
