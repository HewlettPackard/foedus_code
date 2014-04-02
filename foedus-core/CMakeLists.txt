cmake_minimum_required(VERSION 2.4)
project(foedus_core)

SET(CMAKE_VERBOSE_MAKEFILE ON)

# Pointers to the root folders of this project.
# These are used in a few places of the following cmake scripts, so we define them *first*.
set(FOEDUS_CORE_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
mark_as_advanced(FOEDUS_CORE_SRC_ROOT)
set(FOEDUS_CORE_SRC_BIN ${CMAKE_CURRENT_BINARY_DIR})
mark_as_advanced(FOEDUS_CORE_SRC_BIN)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CpplintWrapper.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/WarnInSrcBuild.cmake)

# We do assume C++11.
# However, external projects can link to this library even if they use C++98.
# Our public header files do the switching (hides some API if C++11 is disabled).
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-std\\=c\\+\\+11")
    message(STATUS "The external project that invokes this CMake script disables c++11."
        " libfoedus-core can be still used as a shared library, but some API will be disabled.")
    # this set(CMAKE_CXX_FLAGS) is directory-wide, so it does not affect external projects
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif ()

# We merely collect paths of all header files to install. No executable/library built under include.
add_subdirectory(include)

# We merely collect paths of all cpp files to compile. No executable/library built under src.
add_subdirectory(src)

add_subdirectory(third_party)

# Compile all files as a single shared library
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/glog-0.3.3/src)
add_library(foedus-core SHARED "${FOEDUS_CORE_ALL_SRC}")

# Link to glog
add_dependencies(foedus-core glog_build)
target_link_libraries(foedus-core "${GLOG_STATIC_LIB}")

# Invoke CPPLINT (-DDISABLE_CPPLINT can disable it)
add_dependencies(foedus-core FOEDUS_CORE_CPPLINT_INCLUDE FOEDUS_CORE_CPPLINT_SRC)

find_package(Doxygen)
if(DOXYGEN_FOUND)
    set(DOXYGEN_TARGETS "${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/src")
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(dox
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM)
    add_custom_target(browse-dox
        COMMAND google-chrome ${CMAKE_CURRENT_BINARY_DIR}/dox/html/index.html
        DEPENDS dox)
else(DOXYGEN_FOUND)
    add_custom_target(dox ${CMAKE_COMMAND} -E echo "Doxygen not found. No documents generated.")
endif(DOXYGEN_FOUND)
