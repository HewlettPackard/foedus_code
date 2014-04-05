# We use gflags for command line argument parsing.
# boost::program_options provides equivalent functionalities, but it's not header-only!
# Rather, we statically link to gflags. So far, we are boost-free.
add_subdirectory(gflags-2.1.1)

# For testing, we use Google-testing. It's active, provides more functionalities,
# and has no linking issues.
add_subdirectory(gtest-1.7.0)
