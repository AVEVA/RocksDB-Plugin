# Taken from: https://github.com/bemanproject/exemplar/blob/main/infra/cmake/beman-install-library-config.cmake
include_guard(GLOBAL)

function(aveva_install_library name)
  if (NOT TARGET "${name}")
    message(FATAL_ERROR "Target '${name}' does not exist.")
  endif()

  if (NOT ARGN STREQUAL "")
    message(FATAL_ERROR "aveva_install_library does not accept extra arguments: ${ARGN}")
  endif()

  # Given foo-bar, the component name is bar
  string(REPLACE "-" ";" name_parts "${name}")

  # Fail if the name doesn't look like foo.bar
  list(LENGTH name_parts name_parts_length)
  if (NOT name_parts_length GREATER_EQUAL 2)
    message(FATAL_ERROR "aveva_install_library expects a name of the form 'aveva-<name>', got '${name}'")
  endif()

  set(target_name "${name}")
  set(install_component_name "${name}")
  set(export_name "${name}")
  set(package_name "${name}")
  
  # Ensure the name starts with "aveva-"
  string(FIND "${name}" "aveva-" prefix_index)
  if (NOT prefix_index EQUAL 0)
    message(FATAL_ERROR "Target name must start with 'aveva-', got '${name}'")
  endif()

  # Extract everything after "aveva-"
  string(LENGTH "aveva-" prefix_length)
  string(SUBSTRING "${name}" ${prefix_length} -1 component_name)

  message(STATUS "Target name: ${target_name}")
  message(STATUS "Export name: ${export_name}")
  message(STATUS "Package name: ${package_name}")
  message(STATUS "Install component name: ${install_component_name}")
  message(STATUS "Component name: ${component_name}")

  install(
    TARGETS "${target_name}" COMPONENT "${install_component_name}"
    EXPORT "${export_name}"
    FILE_SET HEADERS
  )

  set_target_properties(
    "${target_name}" PROPERTIES
    EXPORT_NAME "${component_name}"
    VERIFY_INTERFACE_HEADER_SETS ON
  )

  include(GNUInstallDirs)

  # Determine the prefix for project-specific variables
  string(TOUPPER "${name}" project_prefix)
  string(REPLACE "-" "_" project_prefix "${project_prefix}")

  option(
    ${project_prefix}_INSTALL_CONFIG_FILE_PACKAGE
    "Enable building examples. Default: ${PROJECT_IS_TOP_LEVEL}. Values: { ON, OFF }."
    ${PROJECT_IS_TOP_LEVEL}
  )

  # By default, install the config package
  set(install_config_package ON)

  # Turn OFF installation of config package by default if,
  # in order of precedence:
  # 1. The specific package variable is set to OFF
  # 2. The package name is not in the list of packages to install config files
  if(DEFINED aveva_INSTALL_CONFIG_FILE_PACKAGES)
    if(
      NOT "${install_component_name}"
          IN_LIST
          aveva_INSTALL_CONFIG_FILE_PACKAGES
    )
        set(install_config_package OFF)
    endif()
  endif()

  if(DEFINED ${project_prefix}_INSTALL_CONFIG_FILE_PACKAGE)
    set(install_config_package
      ${${project_prefix}_INSTALL_CONFIG_FILE_PACKAGE}
    )
  endif()

  if(install_config_package)
    message(
      DEBUG
      "aveva_install_library: Installing a config package for '${name}'"
    )

    include(CMakePackageConfigHelpers)

    find_file(
      config_file_template
      NAMES "${package_name}-config.cmake.in"
      PATHS "${CMAKE_CURRENT_SOURCE_DIR}"
      NO_DEFAULT_PATH
      NO_CACHE
      REQUIRED
    )
    set(config_package_file
      "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake"
    )

    set(package_install_dir "${name}")
    set(package_install_dir "${CMAKE_INSTALL_PREFIX}/share/${package_install_dir}")
    configure_package_config_file(
      "${config_file_template}"
      "${config_package_file}"
      INSTALL_DESTINATION "${package_install_dir}"
      PATH_VARS PROJECT_NAME PROJECT_VERSION
    )

    set(config_version_file
      "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
    )
    write_basic_package_version_file(
      "${config_version_file}"
      VERSION "${PROJECT_VERSION}"
      COMPATIBILITY ExactVersion
    )

    install(
      FILES "${config_package_file}" "${config_version_file}"
      DESTINATION "${package_install_dir}"
      COMPONENT "${install_component_name}"
    )

    set(config_targets_file "${package_name}-targets.cmake")
    install(
      EXPORT "${export_name}"
      DESTINATION "${package_install_dir}"
      NAMESPACE aveva::
      FILE "${config_targets_file}"
      COMPONENT "${install_component_name}"
    )
  else()
    message(
      DEBUG
      "aveva_install_library: Not installing a config package for '${name}'"
    )
  endif()
endfunction()
