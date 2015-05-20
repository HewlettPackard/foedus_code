# these are third party code, we don't care warning.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

set(BUILD_TESTING OFF) # Disable testcases in these third party libraries

# We use gflags for command line argument parsing.
# boost::program_options provides equivalent functionalities, but it's not header-only!
# Rather, we statically link to gflags. So far, we are boost-free.
set(BUILD_SHARED_LIBS OFF) # we use gflags as static library only
set(BUILD_STATIC_LIBS ON)
add_subdirectory(gflags-2.1.1)

# For testing, we use Google-testing. It's active, provides more functionalities,
# and has no linking issues.
set(BUILD_SHARED_LIBS ON) # To reduce linking time, we now use shared-library version of gtest,
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC") # which requires fPIC as usual. duh.
add_subdirectory(gtest-1.7.0)
