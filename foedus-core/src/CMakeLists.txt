add_subdirectory(foedus)

# Note that PARENT_SCOPE can go up only one level. We define a variable in each child for that.
set(FOEDUS_CORE_ALL_SRC ${FOEDUS_SRC} PARENT_SCOPE)
