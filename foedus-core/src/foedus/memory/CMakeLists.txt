set_property(GLOBAL APPEND PROPERTY ALL_FOEDUS_CORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/aligned_memory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/engine_memory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/memory_options.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/numa_core_memory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/numa_node_memory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/page_pool.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/page_pool_pimpl.cpp
)
