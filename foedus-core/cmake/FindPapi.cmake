# Find the PAPI library.
# Output variables:
#  PAPI_INCLUDE_DIR : e.g., /usr/include/.
#  PAPI_STATIC_LIBRARY     : Library path of PAPI static library
#  PAPI_DYNAMIC_LIBRARY    : Library path of PAPI dynamic library
#  PAPI_FOUND       : True if found.
FIND_PATH(PAPI_INCLUDE_DIR NAME papi.h
  PATHS $ENV{HOME}/local /usr/local/include /usr/include)

FIND_LIBRARY(PAPI_STATIC_LIBRARY NAME libpapi.a
  PATHS $ENV{HOME}/local /usr/local/lib64 /usr/lib64 /usr/local/lib /usr/lib
)
FIND_LIBRARY(PAPI_DYNAMIC_LIBRARY NAME papi
  PATHS $ENV{HOME}/local /usr/local/lib64 /usr/lib64 /usr/local/lib /usr/lib
)

IF (PAPI_INCLUDE_DIR AND PAPI_STATIC_LIBRARY AND PAPI_DYNAMIC_LIBRARY)
    SET(PAPI_FOUND TRUE)
    MESSAGE(STATUS "Found PAPI library: inc=${PAPI_INCLUDE_DIR}, static=${PAPI_STATIC_LIBRARY}, dynamic=${PAPI_DYNAMIC_LIBRARY}")
ELSE ()
    SET(PAPI_FOUND FALSE)
    MESSAGE(STATUS "WARNING: PAPI library not found.")
    MESSAGE(STATUS "Try: 'sudo yum install papi papi-devel papi-static'")
ENDIF ()
