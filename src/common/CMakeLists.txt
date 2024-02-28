add_library(
    wrapperfs_common
    OBJECT
    config.cpp
)

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:wrapperfs_common> PARENT_SCOPE)