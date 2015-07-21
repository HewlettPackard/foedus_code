cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(xxhash)

# I don't find xxHash's release version info. Let's say r39 means 0.39.
set(XXHASH_LIB_VERSION "0.39.0")
set(XXHASH_LIB_SOVERSION "0")

set(BUILD_STATIC_LIBS ON CACHE BOOL "Set to ON to build static libraries")
if(BUILD_STATIC_LIBS)
  add_library(xxhashstatic xxhash.c)
  set_target_properties(xxhashstatic PROPERTIES OUTPUT_NAME xxhash)
endif(BUILD_STATIC_LIBS)

add_library(xxhash SHARED xxhash.c)
set_target_properties(xxhash PROPERTIES
        COMPILE_DEFINITIONS "XXHASH_EXPORT"
 VERSION "${XXHASH_LIB_VERSION}"
 SOVERSION "${XXHASH_LIB_SOVERSION}")

