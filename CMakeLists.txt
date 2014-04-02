cmake_minimum_required(VERSION 2.4)
project(foedus_super_build)

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

add_subdirectory(foedus-core)
add_subdirectory(experiments-core)
add_subdirectory(tests-core)
