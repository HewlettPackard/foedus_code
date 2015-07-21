# This cmake scripts gives a warning or possibly a error if you are doing in-source build.
# See http://www.cmake.org/Wiki/CMake_FAQ#Out-of-source_build_trees for why we do so.
# This is essentially just CMAKE_DISABLE_SOURCE_CHANGES and CMAKE_DISABLE_IN_SOURCE_BUILD.
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

# Note that these do not prohibit building in a directory under the source. For example,
# "ccmake . && make" is prohibited, but "mkdir build && cd build && ccmake ../ && make" is ok.
# The true out-of-source would be to build it in a completely remote place, say ~/tmp/build.
# We don't demand that far.
