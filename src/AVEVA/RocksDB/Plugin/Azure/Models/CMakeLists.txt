add_library(aveva-rocksdb-plugin-azure-models
    ChainedCredentialInfo.cpp
    ServicePrincipalStorageInfo.cpp
)
add_library(aveva::rocksdb-plugin-azure-models ALIAS aveva-rocksdb-plugin-azure-models)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../include")
target_sources(aveva-rocksdb-plugin-azure-models
  PUBLIC
  FILE_SET HEADERS
  BASE_DIRS "${base-include-dir}"
  FILES
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
    "${base-include-dir}/AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
)
install(TARGETS aveva-rocksdb-plugin-azure-models
    EXPORT
)
find_package(aveva-install-library CONFIG REQUIRED)
aveva_install_library(aveva-rocksdb-plugin-azure-models)
target_compile_features(aveva-rocksdb-plugin-azure-models PUBLIC cxx_std_23)
set_target_properties(aveva-rocksdb-plugin-azure-models PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
