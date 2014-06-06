
add_subdirectory(assorted)
add_subdirectory(cache)
add_subdirectory(debugging)
add_subdirectory(externalize)
add_subdirectory(fs)
add_subdirectory(log)
add_subdirectory(memory)
add_subdirectory(restart)
add_subdirectory(savepoint)
add_subdirectory(storage)
add_subdirectory(snapshot)
add_subdirectory(thread)
add_subdirectory(xct)

set_property(GLOBAL APPEND PROPERTY ALL_FOEDUS_CORE_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/assert_nd.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine_options.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine_pimpl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/epoch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/error_stack.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/error_stack_batch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/initializable.cpp
)
