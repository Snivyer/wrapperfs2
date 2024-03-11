#include "wrapper/inode.h"

namespace wrapperfs {


inline static void BuildRnodeKey(size_t ino, rnode_key &key) {
    key.rnode_id = ino;
}


RnodeHandle::RnodeHandle(LevelDBAdaptor* adaptor) {
    this->adaptor = adaptor;
    buff.clear();
}

RnodeHandle::~RnodeHandle() {
    buff.clear();
    this->adaptor = nullptr;
}

void RnodeHandle::change_stat(size_t ino, rnode_status state) {
    buff[ino].stat = state;
}

void RnodeHandle::write_rnode(size_t ino, rnode_header* &rh, rnode_status state) {
    buff[ino].rh = rh;
    buff[ino].stat = state;
}


bool RnodeHandle::get_rnode(size_t ino, rnode_header* &rh) {

    io_s.metadata_read += 1;

    auto ret = buff.find(ino);
    if(ret != buff.end()) {
        io_s.metadata_cache_hit += 1;

        rh = ret->second.rh;
        if(ret->second.stat != rnode_status::remove) {
            return true;
        } else {
            return false;
        }
     
    }

    rnode_key key;
    BuildRnodeKey(ino, key);
    std::string rval = std::string(reinterpret_cast<const char*>(rh), sizeof(rnode_header));

    if (!adaptor->GetValue(key.ToString(), rval)) {
        if(ENABELD_LOG) {
            spdlog::error("get rnode metadata rnode_id - {}: rnode metadata doesn't exist", ino);
        }
            return false;
    }
    io_s.metadata_cache_miss += 1;
    write_rnode(ino, rh, rnode_status::read);
    return true;
}

bool RnodeHandle::put_rnode(size_t ino) {
    
    io_s.metadata_write += 1;

    rnode_key key;
    BuildRnodeKey(ino, key);
    std::string rval = std::string(reinterpret_cast<const char*>(buff[ino].rh), sizeof(rnode_header));

    if (!adaptor->Insert(key.ToString(), rval)) {
        if(ENABELD_LOG) {
            spdlog::error("put rnode metadata rnode_id - {}: kv store interanl error", ino);
        }
        return false;
    }
    change_stat(ino, rnode_status::read);
    return true;  
}

bool RnodeHandle::delete_rnode(size_t ino) {

    io_s.metadata_delete += 1;

    if(buff[ino].rh) {
        delete buff[ino].rh;
    }
    buff.erase(ino);

    rnode_key key;
    BuildRnodeKey(ino, key);
    if(!adaptor->Remove(key.ToString())) {
        spdlog::error("delete rnode metadata rnode_id - {}: kv store interanl error", ino);
        return false;
    }
    return true;
}

bool RnodeHandle::sync(size_t ino) {

    if(buff[ino].stat == rnode_status::write) {
        return put_rnode(ino);
    }

    if(buff[ino].stat == rnode_status::remove) {
        return delete_rnode(ino);
    }

    return true;
}

}