#include "utils/hash_routine.h"

namespace wrapperfs {

std::string generate_hash_string(const std::string &base_string, size_t size) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, base_string.c_str(), base_string.length());
    SHA256_Final(hash, &sha256);
    std::ostringstream ss;
    ss << std::hex << std::setw(2) << std::setfill('0');
    for (size_t i = 0; i < size / 2 && i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::setw(2) << static_cast<int>(hash[i]);
    }
    return ss.str();
}

}