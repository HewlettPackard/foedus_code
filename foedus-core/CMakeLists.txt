#############################################################################
#
#               libfoedus-core CMake script
#
# This CMake project is a bit special because we expect user programs to
# directly include this cmake script to build together as a possible use case.
# Therefore, this CMake script must be compilable even if it was NOT invoked
# by the super build.
#
#############################################################################
cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(foedus_core)

# FIRST, prohibit in-source build.
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/WarnInSrcBuild.cmake)

# Pointers to the root folders of this project.
# These are used in a few places of the following cmake scripts, so we define them here.
set(FOEDUS_CORE_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(FOEDUS_CORE_SRC_BIN ${CMAKE_CURRENT_BINARY_DIR})
mark_as_advanced(FOEDUS_CORE_SRC_ROOT FOEDUS_CORE_SRC_BIN)

set(CMAKE_VERBOSE_MAKEFILE ON)
message (STATUS)
message (STATUS "Building libfoedus-core... Environments:")
message (STATUS "========================================")
message (STATUS "CMAKE_SYSTEM=${CMAKE_SYSTEM}")
message (STATUS "CMAKE_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}")
message (STATUS "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
message (STATUS "CMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
message (STATUS "CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
message (STATUS "========================================")
message (STATUS)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)


#############################################################################
#
#               Compiler Options  : BEGIN
#
#############################################################################
# We crucially need libnuma, so it's a required dependency.
# We considered static linking, but statically linking such a low-level library is a bit risky.
find_package(Numa REQUIRED)

# We also need pthread. Note that linking against libstdc with C++11 flag is not enough.
find_package(Threads REQUIRED)

# google-perftools is optional. We use it only for performance experiments.
# Every such code should be within #ifdef HAVE_GOOGLEPERFTOOLS.
find_package(GooglePerftools)
if (GOOGLEPERFTOOLS_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAVE_GOOGLEPERFTOOLS")
endif (GOOGLEPERFTOOLS_FOUND)

# We do assume C++11.
# However, external projects can link to this library even if they use C++98.
# Our public header files do the switching (hides some API if C++11 is disabled).
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-std\\=c\\+\\+11")
    message(STATUS "The external project that invokes this CMake script disables c++11."
        " libfoedus-core can be still used as a shared library, but some API will be disabled.")
    # this set(CMAKE_CXX_FLAGS) is directory-wide, so it does not affect external projects
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif ()

# We anyway use 64-bit file offsets, but ideally should be consistent with client program's.
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-D\\_FILE\\_OFFSET\\_BITS\\=64")
    message(STATUS "Automatically added -D_FILE_OFFSET_BITS=64 for libfoedus-core."
        " Your client program should also enable the compile option.")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64")
endif ()

# Enable GCC's built-in 128bit CAS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")

set(CMAKE_INCLUDE_CURRENT_DIR OFF)

#############################################################################
#
#               Build Target Libraries  : BEGIN
#
#############################################################################
# include files
include_directories(
    # our main include folder
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    # libnuma
    ${NUMA_INCLUDE_DIR}
    # tinyxml2
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/tinyxml2-2.1.0
    # We include the BINARY folder of glog as glog generates header files
    ${CMAKE_CURRENT_BINARY_DIR}/third_party/glog/src)
# gperftools is optional for this library
if (GOOGLEPERFTOOLS_FOUND)
    include_directories(${GooglePerftools_INCLUDE_DIR})
endif (GOOGLEPERFTOOLS_FOUND)

# This project contains glog so that it can compile by itself.
# We also use gflags and gtest, but this core library itself is a pure data processing library
# without executables nor unit tests. So, we don't contain them in her and instead have them
# in one above.
add_subdirectory(third_party)

# Collect all cpp files in the main source folder.
add_subdirectory(src)
add_library(foedus-core SHARED ${FOEDUS_CORE_ALL_SRC})

# Link to libglog, libnuma, libgperftools (if found)
add_dependencies(foedus-core glog_build)
target_link_libraries(foedus-core ${GLOG_STATIC_LIB})
target_link_libraries(foedus-core ${NUMA_LIBRARY})
target_link_libraries(foedus-core tinyxml2static)
target_link_libraries(foedus-core ${CMAKE_THREAD_LIBS_INIT})
if (GOOGLEPERFTOOLS_FOUND)
    target_link_libraries(foedus-core ${GooglePerftools_LIBRARIES})
endif (GOOGLEPERFTOOLS_FOUND)

# Invoke CPPLINT (if ENABLE_CPPLINTis given)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CpplintWrapper.cmake)
CPPLINT_RECURSIVE(cpplint_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
CPPLINT_RECURSIVE(cpplint_src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/src)
add_dependencies(foedus-core cpplint_include cpplint_src)

#############################################################################
#
#               Doxygen: BEGIN
#
#############################################################################
# Doxygen commands (not mandatory)
find_package(Doxygen)
if(DOXYGEN_FOUND)
    set(DOXYGEN_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/include")
    set(DOXYGEN_TARGETS "${CMAKE_CURRENT_SOURCE_DIR}/include")
    set(DOXYGEN_TARGETS "${DOXYGEN_TARGETS} ${CMAKE_CURRENT_SOURCE_DIR}/src")
    set(DOXYGEN_TARGETS "${DOXYGEN_TARGETS} ${NUMA_INCLUDE_DIR}/numa.h")
    # include other dependencies if it's worth doing. Most of them are not.
    # set(DOXYGEN_TARGETS "${DOXYGEN_TARGETS} ${CMAKE_CURRENT_BINARY_DIR}/third_party/glog/src") # too big

    # Doxygen until ver 1.8.6 had a bug; it allowed only a filename in USE_MDFILE_AS_MAINPAGE,
    # not a full path. see https://bugzilla.gnome.org/show_bug.cgi?id=720092
    # To work it around, we copy the file to bin folder and use only the file name
    set(DOXYGEN_MAINPAGE_MARKDOWN "README.markdown")
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/README.markdown ${CMAKE_CURRENT_BINARY_DIR}/README.markdown COPYONLY)
    # set(DOXYGEN_MAINPAGE_MARKDOWN "${CMAKE_CURRENT_SOURCE_DIR}/README.markdown")

    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(dox
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM)
    add_custom_target(browse-dox
        COMMAND google-chrome ${CMAKE_CURRENT_BINARY_DIR}/dox/html/index.html
        DEPENDS dox)
    # I recommend "yum install texlive texlive-*" to make this work. it requires a few
    # additional sty files.
    add_custom_target(pdf-dox
        COMMAND epstopdf *.eps # sudo yum install texlive-eps*
        COMMAND pdflatex refman.tex # sudo yum install texlive
        COMMAND makeindex refman.idx
        COMMAND pdflatex refman.tex
        COMMAND okular refman.pdf
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dox/latex/
        COMMENT "Generating a single PDF with Doxygen"
        DEPENDS dox)
else(DOXYGEN_FOUND)
    add_custom_target(dox ${CMAKE_COMMAND} -E echo "Doxygen not found. No documents generated.")
endif(DOXYGEN_FOUND)
