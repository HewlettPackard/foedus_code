# Unit test project for foedus-core
cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(tests_core)

include(${CMAKE_CURRENT_SOURCE_DIR}/../general_projects.cmake)
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gtest-1.7.0/include)

# Not used any more.
# CMake function to add compile target, linking, add_test directives.
#function (add_foedus_test targetname)
#   add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${targetname}.cpp)
#   target_link_libraries(${targetname} foedus-core gtest_main test_common)
#
#   add_test(
#        NAME ${targetname}
#        COMMAND ${targetname} --gtest_output=xml:test-reports/result_${targetname}.xml)
#endfunction()

# This one adds individual tests as individual add_test entity, thus showing up nicely in
# results. You should prefer this one. However, you have to list individual tests.
# Automating this is possible, but tricky and I wouldn't rely on it.
#  param: individual_tests a _list_ variable to contain names of individual tests
# Related efforts:
#   http://baoilleach.blogspot.com/2013/06/using-ctest-with-multiple-tests-in.html
#   http://smspillaz.wordpress.com/2012/07/05/unit-test-autodiscovery-with-cmake-and-google-test/
function (add_foedus_test_individual targetname individual_tests)
   add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${targetname}.cpp)
   target_link_libraries(${targetname} foedus-core gtest_main test_common ${CMAKE_THREAD_LIBS_INIT})

   foreach(individual_test ${individual_tests})
        # To pick up test report xml files from Jenkins,
        # Set "test-reports/*.xml" as the report file filter.
        add_test(
            NAME ${targetname}_${individual_test}
            COMMAND ${targetname}
                --gtest_filter=*.${individual_test}
                --gtest_output=xml:${FOEDUS_TEST_REPORT_DIR}/${targetname}_${individual_test}.xml)

        # also add valgrind version. This version takes long, so you might want to exclude
        # them from ctest by "ctest -E valgrind".
        # To pick up valgrind report xml files from Jenkins,
        # Set "valgrind-reports/*.xml" as the valgrind plugin filter.
        add_test(
            NAME valgrind_${targetname}_${individual_test}
            COMMAND
                /usr/bin/valgrind
                --xml=yes --xml-file=${FOEDUS_VALGRIND_REPORT_DIR}/${targetname}_${individual_test}.xml
                --error-exitcode=1 --leak-check=full --show-leak-kinds=all --gen-suppressions=all
                --suppressions=${FOEDUS_CORE_SRC_ROOT}/tools/valgrind.supp
                ./${targetname}
                --gtest_filter=*.${individual_test}
                --gtest_output=xml:${FOEDUS_TEST_REPORT_DIR}/valgrind_${targetname}_${individual_test}.xml
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
   endforeach(individual_test individual_tests)
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
