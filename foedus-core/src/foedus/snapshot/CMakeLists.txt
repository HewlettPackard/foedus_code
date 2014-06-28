set_property(GLOBAL APPEND PROPERTY ALL_FOEDUS_CORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/log_buffer.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_gleaner_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_mapper_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_reducer_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/mapreduce_base_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_manager.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_manager_pimpl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_metadata.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_options.cpp
)
