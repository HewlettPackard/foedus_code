# We use Google's cpplint for style checking.
# This is optional and graceful; meaning we do not quit building even if there are warnings.
# We are not that pedantic. However, we will keep an eye on the count of warnings.
# Related parameters:
#   -DDISABLE_CPPLINT to cmake command will disable cpplint.
find_package(PythonInterp)
option(DISABLE_CPPLINT "Skip cpplint for style checking" OFF)
if(PYTHONINTERP_FOUND)
    if(DISABLE_CPPLINT)
        message(STATUS "-DDISABLE_CPPLINT was given. Skipped running cpplint")
        set(RUN_CPPLINT false)
    else(DISABLE_CPPLINT)
        set(RUN_CPPLINT true)
    endif(DISABLE_CPPLINT)
else(PYTHONINTERP_FOUND)
    message(STATUS "python executable not found. Skipped running cpplint")
    set(RUN_CPPLINT false)
endif(PYTHONINTERP_FOUND)

# Followings are our coding convention.
set(LINT_FILTER) # basically everything Google C++ Style recommends. Except...

# for logging and debug printing, we do need streams
set(LINT_FILTER ${LINT_FILTER},-readability/streams)

# Consider disabling them if they cause too many false positives.
# set(LINT_FILTER ${LINT_FILTER},-build/include_what_you_use)
# set(LINT_FILTER ${LINT_FILTER},-build/include_order)

mark_as_advanced(LINT_FILTER)

set(LINT_SCRIPT "${FOEDUS_CORE_SRC_ROOT}/third_party/cpplint.py")
mark_as_advanced(LINT_SCRIPT)
set(LINT_WRAPPER "${FOEDUS_CORE_SRC_ROOT}/tools/cpplint-wrapper.py")
mark_as_advanced(LINT_WRAPPER)

# 100 chars per line, which is suggested as an option in the style guide
set(LINT_LINELENGTH 100)
mark_as_advanced(LINT_LINELENGTH)

# Registers a CMake target to run cpplint over all files in the given folder.
# Parameters:
#  target_name:
#    The name of the target to define. Your program should depend on it to invoke cpplint.
#  src_folder:
#    The folder to recursively run cpplint.
#  root_folder:
#    The root folder used to determine desired include-guard comments.
#  bin_folder:
#    The temporary build folder to store a cpplint history file.
function(CPPLINT_RECURSIVE target_name src_folder root_folder bin_folder)
    if(RUN_CPPLINT)
        message(STATUS "${target_name}: src=${src_folder}, root=${root_folder}, bin=${bin_folder}")
        file(MAKE_DIRECTORY ${bin_folder})
        add_custom_target(${target_name}
                     COMMAND
                        ${PYTHON_EXECUTABLE} ${LINT_WRAPPER}
                        --cpplint-file=${LINT_SCRIPT}
                        --history-file=${bin_folder}/.lint_history
                        --linelength=${LINT_LINELENGTH}
                        --filter=${LINT_FILTER}
                        --root=${root_folder}
                        ${src_folder}
                     WORKING_DIRECTORY ${bin_folder}
                     COMMENT "[CPPLINT-Target] ${src_folder}")
    else(RUN_CPPLINT)
        add_custom_target(${target_name} COMMAND ${CMAKE_COMMAND} -E echo NoLint)
    endif(RUN_CPPLINT)
endfunction(CPPLINT_RECURSIVE)
