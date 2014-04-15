add_subdirectory(array)
add_subdirectory(hash)
add_subdirectory(masstree)
add_subdirectory(sequential)

set(FOEDUS_STORAGE_CHILD_SRC
    ${FOEDUS_STORAGE_ARRAY_SRC}
    ${FOEDUS_STORAGE_HASH_SRC}
    ${FOEDUS_STORAGE_MASSTREE_SRC}
    ${FOEDUS_STORAGE_SEQUENTIAL_SRC}
)
set(FOEDUS_STORAGE_SRC
    ${FOEDUS_STORAGE_CHILD_SRC}
    ${CMAKE_CURRENT_SOURCE_DIR}/storage_id.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/storage_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/storage_manager_pimpl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/storage_options.cpp
    PARENT_SCOPE)

