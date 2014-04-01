cmake_minimum_required(VERSION 2.4)
project(foedus_core)

# Pointers to the root folders of this project.
# These are used in a few places of the following cmake scripts, so we define them *first*.
set(FOEDUS_CORE_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
mark_as_advanced(FOEDUS_CORE_SRC_ROOT)
set(FOEDUS_CORE_SRC_BIN ${CMAKE_CURRENT_BINARY_DIR})
mark_as_advanced(FOEDUS_CORE_SRC_BIN)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CpplintWrapper.cmake)


# We merely collect paths of all header files to install. No executable/library built under include.
add_subdirectory(include)

# We merely collect paths of all cpp files to compile. No executable/library built under src.
add_subdirectory(src)

add_subdirectory(third_party)
add_subdirectory(tools)

# Compile all files as a single shared library
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
message(STATUS "compile=${FOEDUS_CORE_ALL_CPP}")
add_library(foedus-core SHARED "${FOEDUS_CORE_ALL_CPP}")

# Invoke CPPLINT (-DDISABLE_CPPLINT can disable it)
add_dependencies(foedus-core FOEDUS_CORE_CPPLINT_INCLUDE FOEDUS_CORE_CPPLINT_SRC)

find_package(Doxygen)
if(DOXYGEN_FOUND)
    # We process only headers for doxygen. It should be enough.
    set(DOXYGEN_TARGETS ${CMAKE_CURRENT_SOURCE_DIR}/include)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(dox
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM)
endif(DOXYGEN_FOUND)
