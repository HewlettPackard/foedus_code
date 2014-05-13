#############################################################################
# CMake script used by sub-projects other than foedus-core.
# Those projects are NOT intended to be included from user programs.
#############################################################################
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-std\\=c\\+\\+11")
    message(FATAL_ERROR "These projects assume have to be compiled from super build.")
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/foedus-core/cmake)


find_package(Numa REQUIRED)
find_package(Threads REQUIRED)

include_directories(
    ${CMAKE_SOURCE_DIR}/foedus-core/include
    ${CMAKE_SOURCE_DIR}/third_party/gflags-2.1.1/include
    ${CMAKE_BINARY_DIR}/foedus-core/third_party/glog-0.3.3/src)

# use this in target_link_libraries for the programs
set(GENERAL_LIB foedus-core ${NUMA_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})
