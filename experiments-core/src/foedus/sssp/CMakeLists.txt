add_executable(sssp_datagen ${CMAKE_CURRENT_SOURCE_DIR}/sssp_datagen.cpp)
target_link_libraries(sssp_datagen ${EXPERIMENT_LIB} gflags-static)

add_executable(sssp
  ${CMAKE_CURRENT_SOURCE_DIR}/sssp_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sssp_client_analytic.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sssp_client_nav.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sssp_driver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sssp_hashtable.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sssp_load.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sssp_schema.cpp
  )
target_link_libraries(sssp ${EXPERIMENT_LIB} gflags-static)

add_executable(bitswap_dump ${CMAKE_CURRENT_SOURCE_DIR}/bitswap_dump.cpp)
