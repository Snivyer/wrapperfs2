add_library(
    wrapperfs_wrapper
    OBJECT
    inode.cpp
    wrapper.cpp
)

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:wrapperfs_wrapper> PARENT_SCOPE)