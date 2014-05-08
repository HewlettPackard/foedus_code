add_executable(readonly_experiment ${CMAKE_CURRENT_SOURCE_DIR}/readonly_experiment.cpp)
target_link_libraries(readonly_experiment ${EXPERIMENT_LIB})

add_executable(tpcb_experiment ${CMAKE_CURRENT_SOURCE_DIR}/tpcb_experiment.cpp)
target_link_libraries(tpcb_experiment ${EXPERIMENT_LIB})
