# all test case programas link to this library
add_library(test_common STATIC ${CMAKE_CURRENT_SOURCE_DIR}/test_common.cpp)

add_subdirectory(assorted)
add_subdirectory(fs)
add_subdirectory(memory)
add_subdirectory(storage)
add_subdirectory(thread)
add_subdirectory(xct)

add_foedus_test_individual(test_engine "Instantiate;Initialize;IdempotentUninitialize;Restart;RestartManyTimes")
add_foedus_test_individual(test_engine_options "Instantiate;Copy;Print;SaveLoad")
add_foedus_test_individual(test_epoch "Valid;Comparison")

# header-dependency testcase is not so essential, and it takes long time.
# so, we run it only on jenkins.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DFOEDUS_CORE_SRC_ROOT=${FOEDUS_CORE_SRC_ROOT}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DTINYXML2_SRC_ROOT=${FOEDUS_CORE_SRC_ROOT}/third_party/tinyxml2-2.1.0")
if(RUN_EXPENSIVE_TESTCASES)
    add_foedus_test_individual(test_headers "CheckCompiler;IndependenceCXX11;IndependenceCXX98")
else(RUN_EXPENSIVE_TESTCASES)
    add_foedus_test_individual(test_headers "CheckCompiler")
endif(RUN_EXPENSIVE_TESTCASES)

