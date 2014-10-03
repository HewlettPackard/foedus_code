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
# Also PAPI with same cautions.
find_package(Papi)
if (PAPI_FOUND)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAVE_PAPI")
endif (PAPI_FOUND)

# We do assume C++11.
# However, external projects can link to this library even if they use C++98.
# Our public header files do the switching (hides some API if C++11 is disabled).
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-std\\=c\\+\\+11")
  message(STATUS "The external project that invokes this CMake script disables c++11."
    " libfoedus-core can be still used as a shared library, but some API will be disabled.")
  # this set(CMAKE_CXX_FLAGS) is directory-wide, so it does not affect external projects
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif ()

# We need to make sure -pthread compiler option is specified
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-pthread")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif ()

# We anyway use 64-bit file offsets, but ideally should be consistent with client program's.
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-D\\_FILE\\_OFFSET\\_BITS\\=64")
  message(STATUS "Automatically added -D_FILE_OFFSET_BITS=64 for libfoedus-core."
    " Your client program should also enable the compile option.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64")
endif ()

# We are seeing an weird compiler behavior on union. let's prohibit assuming string aliasing
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-strict-aliasing")

# Enable GCC's built-in 128bit CAS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")

set(CMAKE_INCLUDE_CURRENT_DIR OFF)

#############################################################################
#
#               Build Target Libraries  : BEGIN
#
#############################################################################
# include files
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include) # our main include folder
include_directories(SYSTEM
  # libnuma
  ${NUMA_INCLUDE_DIR}
  # libbacktrace
  ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/libbacktrace
  # tinyxml2
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/tinyxml2-2.1.0
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/glog-0.3.3/src
  # We also include the BINARY folder of glog as glog generates config.h
  ${CMAKE_CURRENT_BINARY_DIR}/third_party/glog-0.3.3/src
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/valgrind-3.9.0/include
  )
# gperftools is optional for this library
if (GOOGLEPERFTOOLS_FOUND)
  include_directories(SYSTEM ${GooglePerftools_INCLUDE_DIR})
endif (GOOGLEPERFTOOLS_FOUND)
# same above
if (PAPI_FOUND)
  include_directories(SYSTEM ${PAPI_INCLUDE_DIR})
endif (PAPI_FOUND)

# This project contains glog so that it can compile by itself.
# We also use gflags and gtest, but this core library itself is a pure data processing library
# without executables nor unit tests. So, we don't contain them in her and instead have them
# in one above.
add_subdirectory(third_party)

# Collect all cpp files in the main source folder.
set_property(GLOBAL PROPERTY ALL_FOEDUS_CORE_SRC "")
add_subdirectory(src)
get_property(TMP_ALL_FOEDUS_CORE_SRC GLOBAL PROPERTY ALL_FOEDUS_CORE_SRC)
add_library(foedus-core SHARED ${TMP_ALL_FOEDUS_CORE_SRC})
add_library(foedus-core-static STATIC ${TMP_ALL_FOEDUS_CORE_SRC})

set(foedus-dependencies)
# Link to libbacktrace, libglog, libnuma, libgperftools (if found)
set(foedus-dependencies ${foedus-dependencies} backtrace)
set(foedus-dependencies ${foedus-dependencies} glog-static)
set(foedus-dependencies ${foedus-dependencies} ${NUMA_LIBRARY})
set(foedus-dependencies ${foedus-dependencies} tinyxml2static)
set(foedus-dependencies ${foedus-dependencies} ${CMAKE_THREAD_LIBS_INIT})
if (GOOGLEPERFTOOLS_FOUND)
  set(foedus-dependencies ${foedus-dependencies} ${GooglePerftools_LIBRARIES})
endif (GOOGLEPERFTOOLS_FOUND)
if (PAPI_FOUND)
  # we use dynamic library of PAPI mainly because of fPIC issue.
  # in terms of license, it's okay to statically link (BSD-style).
  set(foedus-dependencies ${foedus-dependencies} ${PAPI_DYNAMIC_LIBRARY})
endif (PAPI_FOUND)

target_link_libraries(foedus-core ${foedus-dependencies})
target_link_libraries(foedus-core-static ${foedus-dependencies})

# Invoke CPPLINT (if ENABLE_CPPLINT is given)
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
#               Install headers/libraries/documents  : BEGIN
#
#############################################################################
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include" DESTINATION "${CMAKE_INSTALL_PREFIX}")
install(TARGETS foedus-core DESTINATION "${CMAKE_INSTALL_PREFIX}/lib64")
make_directory(${CMAKE_CURRENT_BINARY_DIR}/dox/html)
make_directory(${CMAKE_CURRENT_BINARY_DIR}/dox/latex)
install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/dox/html"
  DESTINATION "${CMAKE_INSTALL_PREFIX}/share/doc/foedus-core")
install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/dox/latex"
  DESTINATION "${CMAKE_INSTALL_PREFIX}/share/doc/foedus-core")

set(CPACK_PACKAGE_VERSION ${FOEDUS_VERSION})
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "foedus-core")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "hideaki.kimura@hp.com")
set(CPACK_PACKAGE_VENDOR "Hewlett Packard")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_RPM_USER_BINARY_SPECFILE "${CMAKE_CURRENT_BINARY_DIR}/rpm.spec")
set(CPACK_TARGET_RPM_DIR "${CMAKE_CURRENT_BINARY_DIR}/_CPack_Packages/Linux")
make_directory(${CPACK_TARGET_RPM_DIR})
make_directory(${CPACK_TARGET_RPM_DIR}/tmp)
make_directory(${CPACK_TARGET_RPM_DIR}/BUILD)
make_directory(${CPACK_TARGET_RPM_DIR}/RPMS)
make_directory(${CPACK_TARGET_RPM_DIR}/SOURCES)
make_directory(${CPACK_TARGET_RPM_DIR}/SPECS)
make_directory(${CPACK_TARGET_RPM_DIR}/SRPMS)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/rpm.spec.cmake" "${CMAKE_CURRENT_BINARY_DIR}/rpm.spec" @ONLY IMMEDIATE)
include(CPack)
# Cheatsheet:
# http://schneide.wordpress.com/2013/02/11/build-a-rpm-package-using-cmake/

#############################################################################
#
#               Doxygen: BEGIN
#
#############################################################################
# Doxygen commands (not mandatory)
find_package(Doxygen)
if(DOXYGEN_FOUND)
  set(DOXYGEN_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/include")
  set(DOXYGEN_LAYOUT "${CMAKE_CURRENT_SOURCE_DIR}/DoxygenLayout.xml")
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
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dox/latex/
    COMMENT "Generating a single PDF with Doxygen"
    DEPENDS dox)
  add_custom_target(browse-pdf-dox
    COMMAND okular refman.pdf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dox/latex/
    DEPENDS pdf-dox)
else(DOXYGEN_FOUND)
  add_custom_target(dox ${CMAKE_COMMAND} -E echo "Doxygen not found. No documents generated.")
endif(DOXYGEN_FOUND)


#############################################################################
#
#               Setting environment: BEGIN
#
#############################################################################
add_custom_target(set-env
  COMMAND sysctl -w kernel.shmmax=9223372036854775807
  COMMAND sysctl -w kernel.shmall=1152921504606846720
  COMMAND sysctl -p
  COMMENT "Sets linux environments for FOEDUS. This is convenient to setup a test/develop environment."
    " You need to run this with sudo. To avoid running this after each reboot, you should also set"
    "the following in /etc/sysctl.conf: kernel.shmmax = 9223372036854775807"
    " kernel.shmall = 1152921504606846720"
  )
