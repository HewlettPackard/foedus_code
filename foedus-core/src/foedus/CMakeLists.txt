
add_subdirectory(fs)
add_subdirectory(log)
add_subdirectory(memory)
add_subdirectory(storage)
add_subdirectory(snapshot)

set(FOEDUS_SRC
    ${FOEDUS_FS_SRC}
    ${FOEDUS_LOG_SRC}
    ${FOEDUS_MEMORY_SRC}
    ${FOEDUS_STORAGE_SRC}
    ${FOEDUS_SNAPSHOT_SRC}
    ${CMAKE_CURRENT_SOURCE_DIR}/engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine_options.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine_pimpl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/epoch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/error_stack.cpp
    PARENT_SCOPE)
