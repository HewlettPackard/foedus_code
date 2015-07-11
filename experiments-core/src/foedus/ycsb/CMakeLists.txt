set(ycsb_cpps
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb_driver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb_load.cpp
)
add_executable(ycsb ${ycsb_cpps})
target_link_libraries(ycsb ${EXPERIMENT_LIB} gflags-static)
