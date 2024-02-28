add_library(
    wrapperfs_adaptor
    OBJECT
    leveldb_adaptor.cpp
)

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:wrapperfs_adaptor> PARENT_SCOPE)