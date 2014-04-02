add_subdirectory(foedus)

# Note that PARENT_SCOPE can go up only one level. We define a variable in each child for that.
set(FOEDUS_CORE_ALL_HDR ${FOEDUS_HDR} PARENT_SCOPE)

# Every file under include is checked by cpplint
CPPLINT_RECURSIVE(FOEDUS_CORE_CPPLINT_INCLUDE
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

