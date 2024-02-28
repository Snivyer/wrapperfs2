add_subdirectory(fs)
add_subdirectory(utils)
add_subdirectory(common)
add_subdirectory(adaptor)
add_subdirectory(wrapper)

add_library(wrapperfs STATIC ${ALL_OBJECT_FILES})

set(WRAPPERFS_LIBS
    wrapperfs_fs
    wrapperfs_utils
    wrapperfs_common
    wrapperfs_adaptor
    wrapperfs_wrapper
)

set(WRAPPERFS_THIRDPARTY_LIBS
    spdlog
    leveldb
    nlohmann_json
)

target_link_libraries(wrapperfs ${WRAPPERFS_LIBS} ${WRAPPERFS_THIRDPARTY_LIBS})

target_include_directories(
    wrapperfs PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)