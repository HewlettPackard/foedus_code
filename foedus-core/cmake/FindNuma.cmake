# Find the numa policy library.
# Output variables:
#  NUMA_INCLUDE_DIR : e.g., /usr/include/.
#  NUMA_LIBRARY     : Library path of numa library
#  NUMA_FOUND       : True if found.
FIND_PATH(NUMA_INCLUDE_DIR NAME numa.h
  PATHS /opt/local/include /usr/local/include /usr/include)

FIND_LIBRARY(NUMA_LIBRARY NAME numa
  PATHS /usr/lib64 /usr/local/lib64 /opt/local/lib64 /usr/lib /usr/local/lib /opt/local/lib
)

IF (NUMA_INCLUDE_DIR AND NUMA_LIBRARY)
    SET(NUMA_FOUND TRUE)
    MESSAGE(STATUS "Found numa library: inc=${NUMA_INCLUDE_DIR}, lib=${NUMA_LIBRARY}")
ELSE ()
    SET(NUMA_FOUND FALSE)
    MESSAGE(STATUS "WARNING: Numa library not found.")
    MESSAGE(STATUS "Try: 'sudo yum install numactl numactl-devel' (or sudo apt-get install libnuma libnuma-dev)")
ENDIF ()
