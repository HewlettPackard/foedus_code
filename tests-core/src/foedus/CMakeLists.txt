# all test case programas link to this library
add_library(test_common STATIC ${CMAKE_CURRENT_SOURCE_DIR}/test_common.cpp)

add_foedus_test(test_engine)
add_foedus_test(test_engine_options)
