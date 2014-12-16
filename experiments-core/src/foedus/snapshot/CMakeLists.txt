add_executable(compose_array_perf ${CMAKE_CURRENT_SOURCE_DIR}/compose_array_perf.cpp)
target_link_libraries(compose_array_perf ${EXPERIMENT_LIB} gflags-static)

add_executable(partition_masstree_perf ${CMAKE_CURRENT_SOURCE_DIR}/partition_masstree_perf.cpp)
target_link_libraries(partition_masstree_perf ${EXPERIMENT_LIB} gflags-static)
