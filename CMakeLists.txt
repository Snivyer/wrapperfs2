cmake_minimum_required(VERSION 3.16.3)

project(wrapperfs CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")
set(CMAKE_BUILD_TYPE Debug)

LINK_LIBRARIES(fuse)

add_compile_options(-D_FILE_OFFSET_BITS=64)

set(WRAPPERFS_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(WRAPPERFS_THIRD_PARTY_INCLUDE_DIR
    ${PROJECT_SOURCE_DIR}/third_party/json/include
    ${PROJECT_SOURCE_DIR}/third_party/spdlog/include
    ${PROJECT_SOURCE_DIR}/third_party/leveldb/include
)

include_directories(${WRAPPERFS_SRC_INCLUDE_DIR} ${WRAPPERFS_THIRD_PARTY_INCLUDE_DIR})

add_subdirectory(src)
add_subdirectory(third_party)
add_subdirectory(tools)