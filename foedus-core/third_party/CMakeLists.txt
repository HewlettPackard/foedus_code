# Google logging has no CMakeLists.txt. So, directly call the configure/make tool chain.
set(GLOG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/glog-0.3.3")
mark_as_advanced(GLOG_DIR)
set(GLOG_BIN_DIR "${CMAKE_CURRENT_BINARY_DIR}/glog")
mark_as_advanced(GLOG_BIN_DIR)
set(GLOG_STATIC_LIB ${GLOG_BIN_DIR}/.libs/libglog.a)
mark_as_advanced(GLOG_STATIC_LIB)

# We invoke make in build folder to keep the glog's source folder clean.
# file(COPY) command requires cmake 2.8 and later. Rather do it with cp command.
# (yes, excluding Windows...)
add_custom_target(glog_copycmd COMMAND cp -ur ${GLOG_DIR} ${CMAKE_CURRENT_BINARY_DIR}/glog)
add_custom_command(OUTPUT ${GLOG_STATIC_LIB}
    # glog code causes a few warnings. As it's not our code, okay to disable its warnings.
    COMMAND ${GLOG_BIN_DIR}/configure CXXFLAGS=-w
    COMMAND ${CMAKE_MAKE_PROGRAM}
    COMMENT [Build-GLOG]
    WORKING_DIRECTORY "${GLOG_BIN_DIR}"
    DEPENDS glog_copycmd ${GLOG_DIR}/ChangeLog # just to see if it has been overwritten
)
# we use add_custom_command for the build itself because otherwise we have to build it
# every time. the following add_custom_target gives a name for the output.
add_custom_target(glog_build DEPENDS ${GLOG_STATIC_LIB})


# Statically import the resulting a file so that we can link to it like:
#   target_link_libraries(my_program glog-static)
add_library(glog-static STATIC IMPORTED)
set_property(TARGET glog-static APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(glog-static PROPERTIES IMPORTED_LOCATION_NOCONFIG "${GLOG_STATIC_LIB}")
add_dependencies(glog-static glog_build)
