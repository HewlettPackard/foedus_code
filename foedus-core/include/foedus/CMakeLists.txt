
add_subdirectory(memory)
add_subdirectory(log)
add_subdirectory(storage)

set(FOEDUS_HDR
    ${FOEDUS_MEMORY_HDR}
    ${CMAKE_CURRENT_SOURCE_DIR}/cxx11.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/namespace-info.hpp
    PARENT_SCOPE)
