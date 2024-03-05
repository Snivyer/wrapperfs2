#pragma once

#include <sys/stat.h>

namespace wrapperfs {

struct file_handle_t {
    int fd;
    int flags;
    size_t ino;
    struct stat stat;
};


struct wrapper_handle_t {
    wrapper_tag tag;
    size_t wrapper_id;
    struct stat stat;
};





}