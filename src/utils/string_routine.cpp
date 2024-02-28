#include "utils/string_routine.h"

namespace wrapperfs {

std::vector<std::string> split_string(const std::string &string, const char* delimiter) {
    std::vector<std::string> tokens;
    char* token = strtok(const_cast<char*>(string.c_str()), delimiter);
    while (token != nullptr) {
        tokens.emplace_back(token);
        token = strtok(nullptr, delimiter);
    }
    return tokens;
}

}