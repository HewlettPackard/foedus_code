set(ycsb_cpps
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb_driver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb_load.cpp
)

# The "normal" version that uses Masstree for storage
add_executable(ycsb_masstree ${ycsb_cpps})
target_link_libraries(ycsb_masstree ${EXPERIMENT_LIB} gflags-static)

# The variant that uses hash table instead of Masstree to store data.
# It doesn't support scan(), i.e., cannot run workload E.
add_executable(ycsb_hash ${ycsb_cpps})
set_target_properties(ycsb_hash PROPERTIES COMPILE_FLAGS "-DYCSB_HASH_STORAGE")
target_link_libraries(ycsb_hash ${EXPERIMENT_LIB} gflags-static)

# Copy the scripts to build folders. this is just for convenience
configure_file(run_dl580.sh run_dl580.sh COPYONLY)
configure_file(run_common.sh run_common.sh COPYONLY)
configure_file(config_ycsb.sh config_ycsb.sh COPYONLY)
configure_file(config_ycsb_f.sh config_ycsb_f.sh COPYONLY)
configure_file(run_ycsb_f_dl580.sh run_ycsb_f_dl580.sh COPYONLY)
