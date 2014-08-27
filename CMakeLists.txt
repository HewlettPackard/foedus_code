#############################################################################
#
#               FOEDUS super-build CMake script
#
# This CMake project is used by the developers of FOEDUS.
# It is NOT an intended use to include this CMake project from user CMake project.
# Rather, users should include CMakeLists.txt in foedus-core.
# All sub-projects other than foedus-core are not intended to be compiled with
# user programs. They are utility programs, performance experiments, and unit
# test cases. Users shouldn't have to include such projects.
#
#############################################################################
cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(foedus_super_build)
set(FOEDUS_VERSION "0.1.0")

# No in-source build please. Terminate it as soon as possible if it happens.
include(${CMAKE_CURRENT_SOURCE_DIR}/foedus-core/cmake/WarnInSrcBuild.cmake)

set(CMAKE_VERBOSE_MAKEFILE ON)

# A few projects other than foedus-core refer to foedus-core. Give a global variable for them.
set(FOEDUS_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(FOEDUS_BIN_ROOT ${CMAKE_CURRENT_BINARY_DIR})
set(FOEDUS_CORE_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/foedus-core)
set(FOEDUS_CORE_BIN_ROOT ${CMAKE_CURRENT_BINARY_DIR}/foedus-core)
set(FOEDUS_TEST_REPORT_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-reports)
set(FOEDUS_VALGRIND_REPORT_DIR ${CMAKE_CURRENT_BINARY_DIR}/valgrind-reports)
make_directory(${FOEDUS_TEST_REPORT_DIR})
make_directory(${FOEDUS_VALGRIND_REPORT_DIR})
mark_as_advanced(FOEDUS_SRC_ROOT FOEDUS_BIN_ROOT FOEDUS_CORE_SRC_ROOT
  FOEDUS_CORE_BIN_ROOT FOEDUS_TEST_REPORT_DIR FOEDUS_VALGRIND_REPORT_DIR)

# All executables should explicitly link to libpthreads, not just relying on implicit linking.
# http://www.jimmo.org/threads-blocked-in-pthread_cond_signal-on-linux/
find_package(Threads REQUIRED)

# Further, all modules should be compiled with -pthread
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=58929
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

# We do assume C++11.
# However, external projects can link to our shared library even if they use C++98.
# Our public header files do the switching (hides some API if C++11 is disabled).
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Yes, we are serious about watching and addressing compiler warnings.
# However, as different versions of compilers have slight differences in rules, "-Werror" would
# cause much more harm than good. So, we enable them just as warnings, not errors.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# We need this for gperftools. If you see "0000fa1234" or something like that instead of function
# names, most likely it's an issue in gperftools.
# https://code.google.com/p/gperftools/issues/detail?id=562
# https://code.google.com/p/gperftools/issues/detail?id=586
# Disabling pie solved the issue for 90% of methods in libfoedus-core.so, but still some methods
# show only function addresses.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-pie")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fno-pie")

# We don't use exceptions, but we hesitate to actually turn it off with -fno-exceptions.
# If the client program expects exceptions, and if libstdc throws an exception, kaboom.
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

# Always 64-bit file offsets
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64")

# These optional modules are linked in all projects.
# We might be using something other than the ones in /usr/lib, so don't let cmake automatically pick
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${FOEDUS_CORE_SRC_ROOT}/cmake)
find_package(GooglePerftools)
if (GOOGLEPERFTOOLS_FOUND)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAVE_GOOGLEPERFTOOLS")
endif (GOOGLEPERFTOOLS_FOUND)
find_package(Papi)
if (PAPI_FOUND)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAVE_PAPI")
endif (PAPI_FOUND)

# We use cpplint to conform to Google C++ style guide.
set(ENABLE_CPPLINT true)

# A unittest is not run if it's expensive AND it's not essential. If it's an essential test case,
# we should make it run fast! So far only header-dependency tests fall into this category.
# The expensive tests are run on jenkins or manually by developers.
option(RUN_EXPENSIVE_TESTCASES "Whether to run testcases that take long time and is not essential."  OFF)

# Find valgrind executable. prefer in the following order:
#  <home_folder>/local/bin/valgrind  (if needed, install a latest version of valgrind to here)
#  /usr/local/bin/valgrind (or here)
#  /usr/bin/valgrind  (probably you have this by standard package manager)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/tests-core/cmake)
find_package(Valgrind REQUIRED)

# Note 2014 May: The following setting for "ctest -D ExperimentalMemCheck" is obsolete,
# although still left for future use when ctest supports valgrind-style xml output.
# We now use "ctest -R valgrind" (run our own valgrind-version targets) for this purpose.
# See https://issues.jenkins-ci.org/browse/JENKINS-23178?page=com.atlassian.jira.plugin.system.issuetabpanels:all-tabpanel
# for why we switched to roll-our-own solution.
# we use CTest in tests-xxx. run "make test" or "ctest" after cmake
set(MEMORYCHECK_SUPPRESSIONS_FILE "${FOEDUS_CORE_SRC_ROOT}/tools/valgrind.supp"
  CACHE FILEPATH "" FORCE)
configure_file(${MEMORYCHECK_SUPPRESSIONS_FILE} ${FOEDUS_BIN_ROOT}/valgrind.supp COPYONLY)
set(MEMORYCHECK_COMMAND "${VALGRIND_EXEC}" CACHE FILEPATH "" FORCE)
# Configurations for ctest
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/CTestCustom.cmake.in ${CMAKE_BINARY_DIR}/CTestCustom.cmake @ONLY)

enable_testing()
# or: valgrind --leak-check=full --show-leak-kinds=all --gen-suppressions=all --suppressions=../../../valgrind.supp ./hoge
# or: ctest -D ExperimentalMemCheck
include(CTest)

# The core library. This project is also compilable by itself.
add_subdirectory(foedus-core)

# Assorted utility programs for FOEDUS users.
add_subdirectory(foedus-util)

# google-test and google-flags.
# we also use google-logging, but it's placed under foedus-core to make it self-contained.
add_subdirectory(third_party)

# experiments and unit tests.
add_subdirectory(experiments-core)
add_subdirectory(tests-core)

find_package(Doxygen)
if(DOXYGEN_FOUND)
  add_custom_target(dox-all DEPENDS dox
    COMMENT "Generate API documentation with Doxygen for all projects")
else(DOXYGEN_FOUND)
  add_custom_target(dox-all ${CMAKE_COMMAND} -E echo "Doxygen not found. No documents generated.")
endif(DOXYGEN_FOUND)

add_custom_target(sloccount
  COMMAND sloccount --duplicates --wide --details
    # exclude third party code and build folder. sloccount is a bit inflexible for this.
    ${CMAKE_CURRENT_SOURCE_DIR}/experiments-core
    ${CMAKE_CURRENT_SOURCE_DIR}/foedus-core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/foedus-core/src
    ${CMAKE_CURRENT_SOURCE_DIR}/tests-core
    ${CMAKE_CURRENT_SOURCE_DIR}/foedus-util
    > ${CMAKE_CURRENT_BINARY_DIR}/sloccount.ac
  COMMENT "Count source lines of code with sloccount")


# for jenkins Cppcheck plugin, also provide a command to write out xml.
set(CPPLINT_CONVERTER ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cpplint_to_cppcheckxml.py)
add_custom_target(cpplint-for-jenkins
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMAND ${CMAKE_BUILD_TOOL}
    cpplint_include cpplint_src
    experiment_core_cpplint_src experiment_core_cpplint_include
    tests_core_cpplint_src tests_core_cpplint_include
    2>&1| ${CPPLINT_CONVERTER} &> ${CMAKE_CURRENT_BINARY_DIR}/cpplint-cppcheck-result.xml
)

# For convenience
add_custom_target(gitpull WORKING_DIRECTORY ${FOEDUS_SRC_ROOT} COMMAND git pull)
