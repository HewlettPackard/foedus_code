add_executable(tpcb_experiment_hash ${CMAKE_CURRENT_SOURCE_DIR}/tpcb_experiment_hash.cpp)
target_link_libraries(tpcb_experiment_hash ${EXPERIMENT_LIB})

add_executable(kickout_experiment_hash ${CMAKE_CURRENT_SOURCE_DIR}/kickout_experiment_hash.cpp)
target_link_libraries(kickout_experiment_hash ${EXPERIMENT_LIB})
