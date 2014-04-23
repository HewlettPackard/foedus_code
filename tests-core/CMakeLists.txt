# Unit test project for foedus-core
cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(tests_core)

include(${CMAKE_CURRENT_SOURCE_DIR}/../general_projects.cmake)
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gtest-1.7.0/include)

# CMake function to add compile target, linking, add_test directives.
function (add_foedus_test targetname)
   add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${targetname}.cpp)
   target_link_libraries(${targetname} ${GENERAL_LIB} gtest_main)

   # To pick up these xml files from Jenkins,
   # Set "**/test-reports/*.xml" as the report file filter.
   add_test(
        NAME ${targetname}
        COMMAND ${targetname} --gtest_output=xml:test-reports/result_${targetname}.xml)
endfunction()

add_subdirectory(src)

# Testcases probably should have cpplint enabled. So, all codes here are lint-checked.
# But, we might revisit this decision.
include(${FOEDUS_CORE_SRC_ROOT}/cmake/CpplintWrapper.cmake)
CPPLINT_RECURSIVE(tests_core_cpplint_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
CPPLINT_RECURSIVE(tests_core_cpplint_src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/src)
add_custom_target(tests_core_cpplint_dummy ALL)
add_dependencies(tests_core_cpplint_dummy tests_core_cpplint_include tests_core_cpplint_src)
