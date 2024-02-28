#pragma once

#include <sys/stat.h>

namespace wrapperfs {

struct file_handle_t {
    int fd;
    int flags;
    size_t ino;
    struct stat stat;
};

}