#############################################################################
#
#               foedus-util CMake script
#
# This CMake project compiles a couple of utility programs to help FOEDUS users.
# These are executables, not libraries. So, no programming API provided.
#
#############################################################################
# Performance experiment project for foedus-core
cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(foedus_util)

include(${CMAKE_CURRENT_SOURCE_DIR}/../general_projects.cmake)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
set(UTIL_LIB foedus-core gflags-static)

add_subdirectory(src)
