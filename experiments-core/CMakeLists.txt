cmake_minimum_required(VERSION 2.4)
project(experiments_core)

if (NOT "${CMAKE_CXX_FLAGS}" MATCHES "\\-std\\=c\\+\\+11")
    message(FATAL_ERROR "Experiments project assume C++11. You have to compile it from super build.")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif ()
