# Performance experiment project for foedus-core
cmake_minimum_required(VERSION 2.4)
project(experiments_core)

include(${CMAKE_CURRENT_SOURCE_DIR}/../general_projects.cmake)
set(EXPERIMENT_LIB ${GENERAL_LIB})

add_subdirectory(src)


