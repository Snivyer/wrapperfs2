#pragma once

#include <string>
#include <sstream>
#include <sys/stat.h>
#include <map>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "common/config.h"
#include "adaptor/leveldb_adaptor.h"

namespace wrapperfs {


enum metadata_status {
    read,
    write,
    remove,
};


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

struct buff_entry {
    rnode_header* rh;
    metadata_status stat;
};


// inode
class RnodeHandle {
private:
    LevelDBAdaptor* adaptor;
    std::map<size_t, buff_entry> buff;
    bool put_rnode(size_t ino);
    bool delete_rnode(size_t ino);

public:
    RnodeHandle(LevelDBAdaptor* adaptor);
    ~RnodeHandle();

    bool get_rnode(size_t ino, struct rnode_header* &rh);
    void write_rnode(size_t ino, struct rnode_header* &rh, metadata_status state = metadata_status::write);
    void change_stat(size_t ino, metadata_status state = metadata_status::write);
    bool sync(size_t ino);

};

}