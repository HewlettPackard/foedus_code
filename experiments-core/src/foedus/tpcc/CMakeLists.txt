set(tpcc_cpps
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
add_executable(tpcc ${tpcc_cpps})
target_link_libraries(tpcc ${EXPERIMENT_LIB} gflags-static)

# "OLAP version" of the TPC-C benchmark.
# It runs only stock-level and order-status.
# Also, the max number of orderlines per order is 100x higher (750 on average).
add_executable(tpcc_olap ${tpcc_cpps})
set_target_properties(tpcc_olap PROPERTIES COMPILE_FLAGS "-DOLAP_MODE")
target_link_libraries(tpcc_olap ${EXPERIMENT_LIB} gflags-static)

# Copy the scripts to build folders. this is just for convenience
configure_file(run_z820.sh run_z820.sh COPYONLY)
configure_file(run_mid.sh run_mid.sh COPYONLY)
configure_file(run_dl580.sh run_dl580.sh COPYONLY)
configure_file(run_dragonhawk.sh run_dragonhawk.sh COPYONLY)
configure_file(run_common.sh run_common.sh COPYONLY)

# For main-memory experiments, but with logs on NVM
configure_file(run_withlog_z820.sh run_withlog_z820.sh COPYONLY)
configure_file(run_withlog_dl580.sh run_withlog_dl580.sh COPYONLY)
configure_file(run_withlog_mid.sh run_withlog_mid.sh COPYONLY)
configure_file(run_withlog_dragonhawk.sh run_withlog_dragonhawk.sh COPYONLY)
configure_file(run_withlog.sh run_withlog.sh COPYONLY)

# For OLAP experiments
configure_file(run_olap_z820.sh run_olap_z820.sh COPYONLY)
configure_file(run_olap_mid.sh run_olap_mid.sh COPYONLY)
configure_file(run_olap_dragonhawk.sh run_olap_dragonhawk.sh COPYONLY)
configure_file(run_olap.sh run_olap.sh COPYONLY)

# For NVM experiments
configure_file(run_nvm_z820.sh run_nvm_z820.sh COPYONLY)
configure_file(run_nvm_mid.sh run_nvm_mid.sh COPYONLY)
configure_file(run_nvm_dl580.sh run_nvm_dl580.sh COPYONLY)
configure_file(run_nvm_dragonhawk.sh run_nvm_dragonhawk.sh COPYONLY)
configure_file(run_nvm.sh run_nvm.sh COPYONLY)

# For HCC experiments
configure_file(run_hcc_z820.sh run_hcc_z820.sh COPYONLY)
configure_file(run_hcc_dl580.sh run_hcc_dl580.sh COPYONLY)
configure_file(run_hcc_dragonhawk.sh run_hcc_dragonhawk.sh COPYONLY)
configure_file(run_hcc_gryphonhawk.sh run_hcc_gryphonhawk.sh COPYONLY)
configure_file(run_hcc.sh run_hcc.sh COPYONLY)

add_subdirectory(hstore_related)

# SILO scripts are not meant to be used from build folder. Just copy-paste them to your console.
