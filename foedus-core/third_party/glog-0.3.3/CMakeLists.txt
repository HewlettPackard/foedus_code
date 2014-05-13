CMAKE_MINIMUM_REQUIRED(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)

set (PACKAGE_NAME      "glog")
set (PACKAGE_VERSION   "0.3.3")
set (PACKAGE_STRING    "${PACKAGE_NAME} ${PACKAGE_VERSION}")
set (PACKAGE_TARNAME   "${PACKAGE_NAME}-${PACKAGE_VERSION}")
set (PACKAGE_BUGREPORT "https://code.google.com/p/google-glog/issues/")
project("${PACKAGE_NAME}")


option(BUILD_SHARED_LIBS          "Request build of shared libraries."                                       OFF)
option(BUILD_STATIC_LIBS          "Request build of static libraries (default if BUILD_SHARED_LIBS is OFF)." OFF)
if (NOT BUILD_SHARED_LIBS AND NOT BUILD_STATIC_LIBS)
  set (BUILD_STATIC_LIBS ON)
endif ()
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CXX_FLAGS)
  set_property (CACHE CMAKE_BUILD_TYPE PROPERTY VALUE Release)
endif ()

set(GOOGLE_NAMESPACE   "google" CACHE STRING "C++ namespace identifier of glog library.")

if (WIN32 AND NOT CYGWIN)
  set (OS_WINDOWS 1)
else ()
  set (OS_WINDOWS 0)
endif ()

include(CheckIncludeFiles)
include(CheckFunctionExists)
include(CheckLibraryExists)
include(CheckSymbolExists)
include(CheckTypeSize)
include(CheckCXXSourceCompiles)

check_include_files(execinfo.h HAVE_EXECINFO_H)
check_include_files(gflags/gflags.h HAVE_GFLAGS_H)
set(HAVE_LIB_GFLAGS ${HAVE_GFLAGS_H}) # Yes, I'm lazy. check_library_exists doesn't work for c++ lib.
check_include_files(glob.h HAVE_GLOB_H)
check_include_files(inttypes.h HAVE_INTTYPES_H)
check_include_files(pthread.h HAVE_PTHREAD)
check_include_files(pwd.h HAVE_PWD_H)
check_include_files(shlwapi.h HAVE_SHLWAPI_H)
check_include_files(stdint.h HAVE_STDINT_H)
check_include_files(sys/stat.h HAVE_SYS_STAT_H)
check_include_files(sys/syscall.h HAVE_SYS_SYSCALL_H)
check_include_files(sys/time.h HAVE_SYS_TIME_H)
check_include_files(sys/types.h HAVE_SYS_TYPES_H)
check_include_files(sys/ucontext.h HAVE_SYS_UCONTEXT_H)
check_include_files(sys/utsname.h HAVE_SYS_UTSNAME_H)
check_include_files(syslog.h HAVE_SYSLOG_H)
check_include_files(syscall.h HAVE_SYSCALL_H)
check_include_files(ucontext.h HAVE_UCONTEXT_H)
check_include_files(unistd.h HAVE_UNISTD_H)
check_include_files(unwind.h HAVE_UNWIND_H)
check_library_exists(unwind get_static_proc_name "" HAVE_LIB_UNWIND)

# TODO HAVE_LIB_GMOCK, HAVE_LIB_GTEST

check_type_size(__uint16 HAVE___UINT16)
check_type_size(u_int16_t HAVE_U_INT16_T)
check_type_size(uint16_t HAVE_UINT16_T)
check_type_size("void*" SIZEOF_VOID_P)
message(STATUS "SIZEOF_VOID_P=${SIZEOF_VOID_P}")


find_package(Threads)
SET(CMAKE_REQUIRED_LIBRARIES dl ${CMAKE_THREAD_LIBS_INIT})
check_function_exists(dladdr HAVE_DLADDR)
check_symbol_exists(fcntl "unistd.h;fcntl.h" HAVE_FCNTL)
check_symbol_exists(pthread_rwlock_rdlock "pthread.h" HAVE_RWLOCK)
check_symbol_exists(sigaltstack "signal.h" HAVE_SIGALTSTACK)

# Check if the compiler supports using ::operator<<
message(STATUS "Checking if the compiler supports using ::operator<<")
check_cxx_source_compiles("
#include <iostream>
std::ostream& operator<<(std::ostream&, struct s);
int main(int argc, char** argv) {
    using ::operator<<; return 0;;
    return 0;
}"
    HAVE_USING_OPERATOR)
if(HAVE_USING_OPERATOR)
    message(STATUS "Checking if the compiler supports using ::operator<< - supported")
else()
    message(STATUS "Checking if the compiler supports using ::operator<< - not supported")
endif()

# Lookg for __sync_val_compare_and_swap.
# check_symbol_exists/check_function_exists don't work for this.
# Check if the compiler supports using ::operator<<
message(STATUS "Looking for __sync_val_compare_and_swap")
check_cxx_source_compiles("
int main(int argc, char** argv) {
    int a = 0;
    return __sync_val_compare_and_swap(&a, 0, 0);
}"
    HAVE___SYNC_VAL_COMPARE_AND_SWAP)
if(HAVE___SYNC_VAL_COMPARE_AND_SWAP)
    message(STATUS "Looking for __sync_val_compare_and_swap - found")
else()
    message(STATUS "Looking for __sync_val_compare_and_swap - not found")
endif()

# Configure glog/config.h
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/glog/config.h.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/src/glog/config.h)

# Other files are copyonly.
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/glog/log_severity.h
    ${CMAKE_CURRENT_BINARY_DIR}/src/glog/log_severity.h COPYONLY)
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/glog/logging.h
    ${CMAKE_CURRENT_BINARY_DIR}/src/glog/logging.h COPYONLY)
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/glog/raw_logging.h
    ${CMAKE_CURRENT_BINARY_DIR}/src/glog/raw_logging.h COPYONLY)
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/glog/stl_logging.h
    ${CMAKE_CURRENT_BINARY_DIR}/src/glog/stl_logging.h COPYONLY)
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/glog/vlog_is_on.h
    ${CMAKE_CURRENT_BINARY_DIR}/src/glog/vlog_is_on.h COPYONLY)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}/src/)
set(GLOG_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/logging.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raw_logging.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/vlog_is_on.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/utilities.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/demangle.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/symbolize.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/signalhandler.cc
)

if(BUILD_SHARED_LIBS)
    add_library(glog SHARED ${GLOG_SRC})
endif(BUILD_SHARED_LIBS)
if(BUILD_STATIC_LIBS)
    # This doubles the build time if both shared and static lib are build.
    # Although obj-file feature is added in CMake recently, probably it is not worth
    # demanding CMake 2.8 for this code size.
    add_library(glog-static STATIC ${GLOG_SRC})
endif(BUILD_STATIC_LIBS)
