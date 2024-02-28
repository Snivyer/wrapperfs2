#pragma once

#include <string>
#include <iomanip>
#include <openssl/sha.h>

namespace wrapperfs {

std::string generate_hash_string(const std::string &base_string, size_t size);

}