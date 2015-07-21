set_property(GLOBAL APPEND PROPERTY ALL_FOEDUS_CORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/assorted_func.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/atomic_fences.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/protected_boundary.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/raw_atomics.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/rich_backtrace.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/uniform_random.cpp
)
