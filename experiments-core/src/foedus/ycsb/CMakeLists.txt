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
