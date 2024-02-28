#pragma once

#include <vector>
#include <string>
#include <cstring>

namespace wrapperfs {

std::vector<std::string> split_string(const std::string &string, const char* delimiter);

}