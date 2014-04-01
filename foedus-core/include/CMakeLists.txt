add_subdirectory(foedus)

# Every file under include is checked by cpplint
CPPLINT_RECURSIVE(FOEDUS_CORE_CPPLINT_INCLUDE
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

