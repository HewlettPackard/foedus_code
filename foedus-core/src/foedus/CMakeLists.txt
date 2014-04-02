
add_subdirectory(memory)

set(FOEDUS_SRC
    ${FOEDUS_MEMORY_SRC}
    ${CMAKE_CURRENT_SOURCE_DIR}/engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine_options.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine_pimpl.cpp
    PARENT_SCOPE)
