# these are third party code, we don't care warning.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

set(BUILD_TESTING OFF) # Disable testcases in these third party libraries
# We always use them as static libraries.
set(BUILD_SHARED_LIBS OFF)
set(BUILD_STATIC_LIBS ON)

# We use gflags for command line argument parsing.
# boost::program_options provides equivalent functionalities, but it's not header-only!
# Rather, we statically link to gflags. So far, we are boost-free.
add_subdirectory(gflags-2.1.1)

# For testing, we use Google-testing. It's active, provides more functionalities,
# and has no linking issues.
add_subdirectory(gtest-1.7.0)

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
# This is the version in apitrace github as of 2014/10/02
# plus this patch:
#   https://gcc.gnu.org/ml/gcc-patches/2013-10/msg01445.html
# Without this, I got sigsegv in read_uint32() just like this:
#   https://groups.google.com/forum/#!topic/golang-nuts/hSEBs6Blo8U
# I also tried to apply the entire libbacktrace from recent gcc repo, but so many things were
# changed and caused other issues. Hence, just applying this patch.
