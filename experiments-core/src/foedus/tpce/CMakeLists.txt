set(tpce_cpps
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_driver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_load.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_schema.cpp
)
add_executable(tpce ${tpce_cpps})
target_link_libraries(tpce ${EXPERIMENT_LIB} gflags-static)
