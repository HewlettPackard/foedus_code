# all test case programas link to this library
add_library(test_common STATIC ${CMAKE_CURRENT_SOURCE_DIR}/test_common.cpp)

add_foedus_test_individual(test_engine "Instantiate;Initialize;IdempotentUninitialize;Restart;RestartManyTimes")
add_foedus_test_individual(test_engine_options "Instantiate;Copy;Print;SaveLoad")
add_foedus_test_individual(test_epoch "Valid;Comparison")

add_subdirectory(assorted)
add_subdirectory(memory)
add_subdirectory(storage)
add_subdirectory(xct)
