#pragma once

#include <string>
#include <unistd.h>
#include "common/config.h"

namespace wrapperfs {

bool open_file(const std::string &path, int &fd);
bool unlink_file(const std::string &path);
bool truncate_file(const std::string &path, size_t len);
bool read_file(int fd, char* buffer, size_t len, size_t off, ssize_t &size);
bool write_file(int fd, const char* buffer, size_t len, size_t off, ssize_t &size);
bool close_file(int &fd);
std::string combine_path(const std::string &base_path, const std::string &sub_path);

}