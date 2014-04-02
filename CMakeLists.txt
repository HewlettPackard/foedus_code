
cmake_minimum_required(VERSION 2.4)
project(foedus_super_build)

# We do assume C++11.
# However, external projects can link to our shared library even if they use C++98.
# Our public header files do the switching (hides some API if C++11 is disabled).
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

add_subdirectory(foedus-core)
add_subdirectory(experiments-core)
add_subdirectory(tests-core)
