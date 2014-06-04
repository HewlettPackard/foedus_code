# Performance experiment project for foedus-core
cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(experiments_core)

include(${CMAKE_CURRENT_SOURCE_DIR}/../general_projects.cmake)
set(EXPERIMENT_LIB foedus-core ${CMAKE_THREAD_LIBS_INIT})

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(src)

# Experiments code might be exempted from cpplint later when we think it's too tedious,
# but fow now we enable cpplint to experiments code, too.
include(${FOEDUS_CORE_SRC_ROOT}/cmake/CpplintWrapper.cmake)
CPPLINT_RECURSIVE(experiment_core_cpplint_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
CPPLINT_RECURSIVE(experiment_core_cpplint_src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/src)
add_custom_target(experiment_core_cpplint_dummy ALL)
add_dependencies(experiment_core_cpplint_dummy experiment_core_cpplint_include experiment_core_cpplint_src)
