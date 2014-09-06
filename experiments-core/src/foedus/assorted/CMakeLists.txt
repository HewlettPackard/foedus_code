add_executable(prefetch_experiment ${CMAKE_CURRENT_SOURCE_DIR}/prefetch_experiment.cpp)
target_link_libraries(prefetch_experiment ${EXPERIMENT_LIB})

add_executable(l3miss_experiment ${CMAKE_CURRENT_SOURCE_DIR}/l3miss_experiment.cpp)
target_link_libraries(l3miss_experiment ${EXPERIMENT_LIB})

add_executable(l3miss_multip_experiment ${CMAKE_CURRENT_SOURCE_DIR}/l3miss_multip_experiment.cpp)
target_link_libraries(l3miss_multip_experiment ${EXPERIMENT_LIB})
