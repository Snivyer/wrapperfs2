#include "utils/filesystem_routine.h"

namespace wrapperfs {

bool open_file(const std::string &path, int &fd) {
    fd = open(path.c_str(), O_CREAT | O_RDWR);
    return fd >= 0;
}

bool unlink_file(const std::string &path) {
    return unlink(path.c_str()) >= 0;
}

bool truncate_file(const std::string &path, size_t len) {
    return truncate(path.c_str(), len) >= 0;
}

bool read_file(int fd, char* buffer, size_t len, size_t off, ssize_t &size) {
    size = pread(fd, buffer, len, off);
    return size >= 0;
}

bool write_file(int fd, const char* buffer, size_t len, size_t off, ssize_t &size) {
    size = pwrite(fd, buffer, len, off);
    return size >= 0;
}

bool close_file(int &fd) {
    return close(fd) >= 0;
}

std::string combine_path(const std::string &base_path, const std::string &sub_path) {
    std::string new_sub_path = sub_path;
    if (!sub_path.empty() && sub_path.front() == *PATH_DELIMITER) {
        if (sub_path.size() > 1) {
            new_sub_path = sub_path.substr(1);
        } else {
            new_sub_path.clear();
        }
    }
    if (base_path.empty() || base_path.back() == *PATH_DELIMITER) {
        return base_path + new_sub_path;
    } else {
        return base_path + PATH_DELIMITER + new_sub_path;
    }
}

}