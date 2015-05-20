set_property(GLOBAL APPEND PROPERTY ALL_FOEDUS_CORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/log_buffer.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_gleaner_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_gleaner_ref.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_gleaner_resource.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_mapper_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_reducer_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_reducer_ref.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/mapreduce_base_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/merge_sort.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_manager.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_manager_pimpl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_metadata.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_options.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/snapshot_writer_impl.cpp
)
