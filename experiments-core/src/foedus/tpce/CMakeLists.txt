set(tpce_cpps
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_driver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_load.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_schema.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_trade_order.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpce_trade_update.cpp
)
add_executable(tpce ${tpce_cpps})
target_link_libraries(tpce ${EXPERIMENT_LIB} gflags-static)

configure_file(run_hp840.sh run_hp840.sh COPYONLY)
configure_file(run_dh.sh run_dh.sh COPYONLY)
