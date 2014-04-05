# Unit test project for foedus-core
cmake_minimum_required(VERSION 2.4)
project(tests_core)

include(${CMAKE_CURRENT_SOURCE_DIR}/../general_projects.cmake)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gtest-1.7.0/include)

# use this in target_link_libraries for test programs
set(TEST_LIB ${GENERAL_LIB})

add_subdirectory(src)
