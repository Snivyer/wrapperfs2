add_library(
    wrapperfs_utils
    OBJECT
    hash_routine.cpp
    string_routine.cpp
    filesystem_routine.cpp
)

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:wrapperfs_utils> PARENT_SCOPE)