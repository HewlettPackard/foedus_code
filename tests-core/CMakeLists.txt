cmake_minimum_required(VERSION 2.4)
project(tests_core)

if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-std\\=c\\+\\+11")
    message(FATAL_ERROR "Tests project assume C++11. You have to compile it from super build.")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif ()

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../foedus-core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gtest-1.7.0/include)
