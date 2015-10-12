add_executable(sssp_datagen ${CMAKE_CURRENT_SOURCE_DIR}/sssp_datagen.cpp)
target_link_libraries(sssp_datagen ${EXPERIMENT_LIB} gflags-static)

add_executable(sssp_driver ${CMAKE_CURRENT_SOURCE_DIR}/sssp_driver.cpp)
target_link_libraries(sssp_driver ${EXPERIMENT_LIB} gflags-static)
