cmake_minimum_required(VERSION 2.4)
project(foedus_core)

set(CMAKE_VERBOSE_MAKEFILE ON)

# Pointers to the root folders of this project.
# These are used in a few places of the following cmake scripts, so we define them *first*.
set(FOEDUS_CORE_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
mark_as_advanced(FOEDUS_CORE_SRC_ROOT)
set(FOEDUS_CORE_SRC_BIN ${CMAKE_CURRENT_BINARY_DIR})
mark_as_advanced(FOEDUS_CORE_SRC_BIN)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CpplintWrapper.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/WarnInSrcBuild.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/third_party/TestBigEndian.cmake)

#TEST_BIG_ENDIAN(WORDS_BIGENDIAN)
message (STATUS)
message (STATUS "========================================")
message (STATUS "CMAKE_SYSTEM=${CMAKE_SYSTEM}")
message (STATUS "CMAKE_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}")
message (STATUS "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
message (STATUS "CMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
message (STATUS "CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
#message (STATUS "BIG_ENDIAN=${WORDS_BIGENDIAN}")
message (STATUS "========================================")
message (STATUS)

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


# We merely collect paths of all cpp files to compile. No executable/library built under src.
add_subdirectory(src)

# So far only glog.
add_subdirectory(third_party)

# Compile all files as a single shared library
include_directories(
    # our main include folder
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    # We include the BINARY folder of glog as glog generates header files
    ${CMAKE_CURRENT_BINARY_DIR}/third_party/glog/src)
add_library(foedus-core SHARED ${FOEDUS_CORE_ALL_SRC})

# Link to glog
add_dependencies(foedus-core glog_build)
target_link_libraries(foedus-core "${GLOG_STATIC_LIB}")

# Invoke CPPLINT (-DDISABLE_CPPLINT can disable it)
CPPLINT_RECURSIVE(cpplint_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
CPPLINT_RECURSIVE(cpplint_src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/src)
add_dependencies(foedus-core cpplint_include cpplint_src)

# Doxygen commands (not mandatory)
find_package(Doxygen)
if(DOXYGEN_FOUND)
    set(DOXYGEN_TARGETS "${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/src")
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
else(DOXYGEN_FOUND)
    add_custom_target(dox ${CMAKE_COMMAND} -E echo "Doxygen not found. No documents generated.")
endif(DOXYGEN_FOUND)
