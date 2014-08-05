add_executable(tpcc
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_delivery.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_driver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_load.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_neworder.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_order_status.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_payment.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_stock_level.cpp
)
target_link_libraries(tpcc ${EXPERIMENT_LIB} gflags-static)
