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

# No in-source build please. Terminate it as soon as possible if it happens.
include(${CMAKE_CURRENT_SOURCE_DIR}/foedus-core/cmake/WarnInSrcBuild.cmake)

set(CMAKE_VERBOSE_MAKEFILE ON)

# A few projects other than foedus-core refer to foedus-core. Give a global variable for them.
set(FOEDUS_CORE_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/foedus-core)
set(FOEDUS_CORE_SRC_BIN ${CMAKE_CURRENT_BINARY_DIR}/foedus-core)
mark_as_advanced(FOEDUS_CORE_SRC_ROOT FOEDUS_CORE_SRC_BIN)

# We do assume C++11.
# However, external projects can link to our shared library even if they use C++98.
# Our public header files do the switching (hides some API if C++11 is disabled).
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Yes, we are serious about watching and addressing compiler warnings.
# However, as different versions of compilers have slight differences in rules, "-Werror" would
# cause much more harm than good. So, we enable them just as warnings, not errors.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# We don't use exceptions, but we hesitate to actually turn it off with -fno-exceptions.
# If the client program expects exceptions, and if libstdc throws an exception, kaboom.
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

# Always 64-bit file offsets
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64")

# We use cpplint to conform to Google C++ style guide.
set(ENABLE_CPPLINT true)

enable_testing() # we use CTest in tests-xxx. run "make test" or "ctest" after cmake

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
