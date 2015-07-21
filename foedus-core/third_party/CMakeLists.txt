# Google logging has no CMakeLists.txt. So, directly call the configure/make tool chain.
# This is very troublesome and undesired. I hope they will move on to CMake asap.
#set(GLOG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/glog-0.3.3")
#set(GLOG_BIN_DIR "${CMAKE_CURRENT_BINARY_DIR}/glog")
#set(GLOG_STATIC_LIB_LOCAL "${GLOG_BIN_DIR}/.libs/libglog.a")
#mark_as_advanced(GLOG_DIR GLOG_BIN_DIR GLOG_STATIC_LIB_LOCAL)

# We invoke make in build folder to keep the glog's source folder clean.
#file(MAKE_DIRECTORY ${GLOG_BIN_DIR})
#add_custom_command(OUTPUT ${GLOG_STATIC_LIB_LOCAL}
#    COMMAND ${CMAKE_COMMAND} -E copy_directory ${GLOG_DIR} ${CMAKE_CURRENT_BINARY_DIR}/glog
#    # fPIC is needed to build libglog friendly to shared library (libfoedus-core).
#    COMMAND ${GLOG_BIN_DIR}/configure 'CXXFLAGS=-fPIC -w' # also ignore warnings from glog
#    COMMAND ${CMAKE_MAKE_PROGRAM} -j
#    COMMENT [Build-GLOG]
#    WORKING_DIRECTORY "${GLOG_BIN_DIR}"
#    DEPENDS ${GLOG_DIR}/ChangeLog # just to see if it has been overwritten
#)
# we use add_custom_command for the build itself because otherwise we have to build it
# every time. the following add_custom_target gives a name for the output.
#add_custom_target(glog_build ALL DEPENDS ${GLOG_STATIC_LIB_LOCAL})
#set(GLOG_STATIC_LIB ${GLOG_STATIC_LIB_LOCAL} PARENT_SCOPE)

# oh, lovely tinyxml2. It's a CMake project.
set(BUILD_STATIC_LIBS ON) # this creates tinyxml2static library
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -w") # same as glog
add_subdirectory(tinyxml2-2.1.0)
# Glog is now also a CMake project.
add_subdirectory(glog-0.3.3)

# We contain valgrind folder here, but we use it just as a header-only library.
# We only need RUNNING_ON_VALGRIND macro from it.

# We use a variant of libbacktrace to get rich stacktrace information.
# This was retrieved from https://github.com/apitrace/apitrace
# which adds CMake script to the original libbacktrace in gcc.
# All files under libbacktrace folder are under BSD license of the original
# libbacktrace (see README), except cmake-related files under the following
# license (BSD).
## Copyright 2007-2012 VMware, Inc.
## Copyright 2011 Intel Corporation
## Copyright 2011 LunarG, Inc.
## Copyright 2011 Zack Rusin
## Copyright 2011-2012 Jose Fonseca
##
## All Rights Reserved.
##
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in
## all copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
## THE SOFTWARE.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -w") # to statically link from libfoedus.so
add_subdirectory(libbacktrace)
# This is the latest version in https://github.com/gcc-mirror/gcc/tree/master/libbacktrace
# of 2014/10/02.
# I had to apply at least this patch:
#   https://gcc.gnu.org/ml/gcc-patches/2013-10/msg01445.html
# Without this, I got sigsegv in read_uint32() just like this:
#   https://groups.google.com/forum/#!topic/golang-nuts/hSEBs6Blo8U
# In case of other issues happening later, I applied the latest from gcc.

# xxHash library for hash function
set(BUILD_STATIC_LIBS ON) # this creates xxhashstatic library
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -w") # to statically link from libfoedus.so
add_subdirectory(xxHash-r39)

