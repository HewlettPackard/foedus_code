# Google logging has no CMakeLists.txt. So, directly call the configure/make tool chain.
# This is very troublesome and undesired. I hope they will move on to CMake asap.
set(GLOG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/glog-0.3.3")
mark_as_advanced(GLOG_DIR)
set(GLOG_BIN_DIR "${CMAKE_CURRENT_BINARY_DIR}/glog")
mark_as_advanced(GLOG_BIN_DIR)
set(GLOG_STATIC_LIB_LOCAL "${GLOG_BIN_DIR}/.libs/libglog.a")
mark_as_advanced(GLOG_STATIC_LIB_LOCAL)

# We invoke make in build folder to keep the glog's source folder clean.
file(MAKE_DIRECTORY ${GLOG_BIN_DIR})
add_custom_command(OUTPUT ${GLOG_STATIC_LIB_LOCAL}
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${GLOG_DIR} ${CMAKE_CURRENT_BINARY_DIR}/glog
    # fPIC is needed to build libglog friendly to shared library (libfoedus).
    COMMAND ${GLOG_BIN_DIR}/configure CXXFLAGS=-fPIC
    COMMAND ${CMAKE_MAKE_PROGRAM} -j
    COMMENT [Build-GLOG]
    WORKING_DIRECTORY "${GLOG_BIN_DIR}"
    DEPENDS ${GLOG_DIR}/ChangeLog # just to see if it has been overwritten
)
# we use add_custom_command for the build itself because otherwise we have to build it
# every time. the following add_custom_target gives a name for the output.
add_custom_target(glog_build ALL DEPENDS ${GLOG_STATIC_LIB_LOCAL})
set(GLOG_STATIC_LIB ${GLOG_STATIC_LIB_LOCAL} PARENT_SCOPE)
