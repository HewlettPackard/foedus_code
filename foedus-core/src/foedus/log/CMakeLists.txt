set_property(GLOBAL APPEND PROPERTY ALL_FOEDUS_CORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/common_log_types.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/epoch_history.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/logger_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_manager.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_manager_pimpl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_options.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_type.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/log_type_invoke.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/thread_log_buffer_impl.cpp
)
