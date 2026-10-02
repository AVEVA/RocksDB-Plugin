cmake_minimum_required(VERSION 3.22)
project(aveva-azure-client-install-consumer LANGUAGES CXX)

# Resolves the library exactly as an external consumer would: through the installed package config only.
find_package(aveva-azure-client CONFIG REQUIRED)

add_executable(install-consumer main.cpp)
target_link_libraries(install-consumer PRIVATE aveva::azure-client)
