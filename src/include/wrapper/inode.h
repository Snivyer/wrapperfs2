#pragma once

#include <string>
#include <sstream>
#include <sys/stat.h>
#include <unordered_map>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "common/config.h"
#include "adaptor/leveldb_adaptor.h"

namespace wrapperfs {


struct rnode_key {
    char flag;
    size_t rnode_id;

    std::string ToString() {
    std::stringstream s;
    s  << "r:" << rnode_id;
        return s.str();
    }
};

struct rnode_header {
    struct stat fstat;
};


// inode
class RnodeHandle {
private:
    LevelDBAdaptor* adaptor;

    std::unordered_map<size_t, struct stat*> cache;

public:
    RnodeHandle(LevelDBAdaptor* adaptor);
    ~RnodeHandle();

    bool get_rnode(rnode_key &key, std::string &value);
    bool put_rnode(rnode_key &key, std::string value);
    bool delete_rnode(rnode_key &key);

    struct stat* get_rnode(size_t ino);


};

}