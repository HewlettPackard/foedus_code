add_subdirectory(foedus)

# Note that PARENT_SCOPE can go up only one level. We define a variable in each child for that.
set(FOEDUS_CORE_ALL_SRC ${FOEDUS_SRC} PARENT_SCOPE)

# Every file under src is checked by cpplint
CPPLINT_RECURSIVE(FOEDUS_CORE_CPPLINT_SRC
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

