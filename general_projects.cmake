#############################################################################
# CMake script used by sub-projects other than foedus-core.
# Those projects are NOT intended to be included from user programs.
#############################################################################
if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-std\\=c\\+\\+11")
    message(FATAL_ERROR "These projects have to be compiled from super build.")
endif ()

include_directories(
    ${CMAKE_SOURCE_DIR}/foedus-core/include
    ${CMAKE_BINARY_DIR}/third_party/gflags-2.1.1/include
    ${CMAKE_SOURCE_DIR}/foedus-core/third_party/glog-0.3.3/src
    ${CMAKE_BINARY_DIR}/foedus-core/third_party/glog-0.3.3/src)
