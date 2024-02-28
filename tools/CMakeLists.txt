add_executable(fs fs.cpp)
add_executable(mkfs mkfs.cpp)

target_link_libraries(fs wrapperfs)
target_link_libraries(mkfs wrapperfs)