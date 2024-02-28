add_library(
    wrapperfs_fs
    OBJECT
    wrapperfs.cpp
)

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:wrapperfs_fs> PARENT_SCOPE)