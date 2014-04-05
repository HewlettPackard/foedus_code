cmake_minimum_required(VERSION 2.4)
project(foedus_super_build)

# No in-source build please. Terminate it as soon as possible if it happens.
include(${CMAKE_CURRENT_SOURCE_DIR}/foedus-core/cmake/WarnInSrcBuild.cmake)

# We do assume C++11.
# However, external projects can link to our shared library even if they use C++98.
# Our public header files do the switching (hides some API if C++11 is disabled).
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Yes, we are serious about watching and addressing compiler warnings.
# However, as different versions of compilers have slight differences in rurles, "-Werror" would
# cause much more harm than good. So, we enable them just as warnings, not errors.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# We don't use exceptions, but we hesitate to actually turn it off with -fno-exceptions.
# If the client program expects exceptions, and if libstdc throws an exception, kaboom.
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

# Always 64-bit file offsets
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64")

enable_testing() # we use CTest in tests-xxx. run "make test" or "ctest" after cmake

# The core library. This project is also compilable by itself.
add_subdirectory(foedus-core)

# google-test and google-flags.
# we also use google-logging, but it's placed under foedus-core to make it self-contained.
add_subdirectory(third-party)

# experiments and unit tests.
add_subdirectory(experiments-core)
add_subdirectory(tests-core)
