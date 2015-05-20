# Find valgrind binary and get its version
# Output variables:
#
#  VALGRIND_FOUND         True if system has valgrind installed
#  VALGRIND_DIR           Full path of root folder (eg. /usr) that contains valgrind executable
#  VALGRIND_EXEC          Full path of valgrind executable
#  VALGRIND_VERSION       Version of the valgrind (eg "valgrind-3.8.1". what "valgrind --version" returns)
#  VALGRIND_VERSION_MAJOR Numerical major version of the valgrind (eg 3.8.1 => 3)
#  VALGRIND_VERSION_MINOR Numerical minor version of the valgrind (eg 3.8.1 => 8)
#  VALGRIND_VERSION_PATCH Numerical patch version of the valgrind (eg 3.8.1 => 1)

find_path(VALGRIND_DIR
    NAMES bin/valgrind bin64/valgrind
    HINTS $ENV{HOME}/local /usr/local /usr
)

if (VALGRIND_DIR)
    set(VALGRIND_FOUND TRUE)
    set(VALGRIND_EXEC "${VALGRIND_DIR}/bin/valgrind")
    message(STATUS "Found valgrind executable. root dir=${VALGRIND_DIR}, exec=${VALGRIND_EXEC}")
    message(STATUS "Running ${VALGRIND_EXEC} to check version...")
    exec_program(${VALGRIND_EXEC} ARGS --version OUTPUT_VARIABLE VALGRIND_VERSION)
    # parse it
    # Examples. "valgrind-3.9.0", "valgrind-3.10.0.SVN", etc
    string(REGEX REPLACE "valgrind\\-([0-9]+)\\.[0-9]+\\.[0-9]+.*" "\\1" VALGRIND_VERSION_MAJOR "${VALGRIND_VERSION}")
    string(REGEX REPLACE "valgrind\\-[0-9]+\\.([0-9]+)\\.[0-9]+.*" "\\1" VALGRIND_VERSION_MINOR "${VALGRIND_VERSION}")
    string(REGEX REPLACE "valgrind\\-[0-9]+\\.[0-9]+\\.([0-9]+).*" "\\1" VALGRIND_VERSION_PATCH "${VALGRIND_VERSION}")
    message(STATUS "VALGRIND_VERSION_MAJOR=${VALGRIND_VERSION_MAJOR}")
    message(STATUS "VALGRIND_VERSION_MINOR=${VALGRIND_VERSION_MINOR}")
    message(STATUS "VALGRIND_VERSION_PATCH=${VALGRIND_VERSION_PATCH}")
    if ((${VALGRIND_VERSION_MAJOR} LESS 4) AND (${VALGRIND_VERSION_MINOR} LESS 9))
        message(WARNING "The version of the valgrind executable (${VALGRIND_EXEC}) is ${VALGRIND_VERSION}. We recommend at least valgrind 3.9 for your happier development life.")
    elseif (("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "aarch64") AND
      (${VALGRIND_VERSION_MAJOR} LESS 4) AND
      (${VALGRIND_VERSION_MINOR} LESS 11) AND
      (${VALGRIND_VERSION_PATCH} LESS 1))
        message(WARNING "STRONG WARNING on AArch64!!! valgrind 3.10.0 and earlier has a critical bug on AArch64. You must use 3.10.1 or later. The version of the valgrind executable (${VALGRIND_EXEC}) is ${VALGRIND_VERSION}.")
        message(WARNING "There are several missing ARMv8 instructions in 3.10.0, which causes the unhandled instruction errors.")
    else ()
        message(STATUS "Great, the version of the valgrind executable (${VALGRIND_EXEC}) is latest enough: ${VALGRIND_VERSION}.")
    endif()
else ()
    set(VALGRIND_FOUND FALSE)
    message(STATUS "WARNING: valgrind executable not found.")
    message(STATUS "Try: sudo yum install valgrind")
endif ()

mark_as_advanced(
    VALGRIND_FOUND
    VALGRIND_DIR
    VALGRIND_EXEC
)
