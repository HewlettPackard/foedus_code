add_executable(tpcc
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_delivery.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_driver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_load.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_neworder.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_order_status.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_payment.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_replicate_binaries.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_schema.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_stock_level.cpp
)
target_link_libraries(tpcc ${EXPERIMENT_LIB} gflags-static)

# Copy the scripts to build folders. this is just for convenience
configure_file(run_z820.sh run_z820.sh COPYONLY)
configure_file(run_dl580.sh run_dl580.sh COPYONLY)
configure_file(run_dragonhawk.sh run_dragonhawk.sh COPYONLY)
configure_file(run_common.sh run_common.sh COPYONLY)

# For NVM experiments
configure_file(run_nvm_z820.sh run_nvm_z820.sh COPYONLY)
configure_file(run_nvm_dl580.sh run_nvm_dl580.sh COPYONLY)
configure_file(run_nvm_dragonhawk.sh run_nvm_dragonhawk.sh COPYONLY)
configure_file(run_nvm.sh run_nvm.sh COPYONLY)

# For H-store experiments.
configure_file(hstore_tpcc.properties.in hstore_tpcc.properties.in COPYONLY)
configure_file(run_hstore_common.sh run_hstore_common.sh COPYONLY)
configure_file(run_hstore_z820.sh run_hstore_z820.sh COPYONLY)
configure_file(run_hstore_dl580.sh run_hstore_dl580.sh COPYONLY)
configure_file(run_hstore_dragonhawk.sh run_hstore_dragonhawk.sh COPYONLY)

