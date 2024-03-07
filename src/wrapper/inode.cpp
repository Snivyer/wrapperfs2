#include "wrapper/inode.h"

namespace wrapperfs {

struct stat* GetMetadata(std::string &value) {
    return reinterpret_cast<struct stat*> (value.data());
}

inline static void BuildRnodeKey(size_t ino, rnode_key &key) {
    key.rnode_id = ino;
}


RnodeHandle::RnodeHandle(LevelDBAdaptor* adaptor) {
    this->adaptor = adaptor;
    cache.clear();
}

RnodeHandle::~RnodeHandle() {
    cache.clear();
    this->adaptor = nullptr;
}

struct stat* RnodeHandle::get_rnode(size_t ino) {

    io_s.metadata_read += 1;

    auto ret = cache.find(ino);
    if(ret != cache.end()) {
        return ret->second;
    }
    return nullptr;
}


bool RnodeHandle::get_rnode(rnode_key &key, std::string &rval) {

    if (!adaptor->GetValue(key.ToString(), rval)) {
   
        if(ENABELD_LOG) {
            spdlog::error("get rnode metadata rnode_id - {}: rnode metadata doesn't exist", key.ToString());
        }
            return false;
    }

    io_s.entry_write += 1;
    io_s.metadata_cache_miss += 1;

    // 加入缓存
    cache.insert({key.rnode_id, GetMetadata(rval)});
    io_s.metadata_cache_replace += 1;
  
    return true;
}

bool RnodeHandle::put_rnode(rnode_key &key, std::string rval) {
    
    io_s.metadata_write += 1;

    if (!adaptor->Insert(key.ToString(), rval)) {
        if(ENABELD_LOG) {
            spdlog::error("put rnode metadata rnode_id - {}: kv store interanl error", key.ToString());
        }
        return false;
    }

    // 加入缓存
    cache[key.rnode_id] = GetMetadata(rval);
    io_s.metadata_cache_replace += 1;
    return true;

}

bool RnodeHandle::delete_rnode(rnode_key &key) {

    io_s.metadata_delete += 1;

    auto ret = cache.find(key.rnode_id);
    if(ret != cache.end()) {
        cache.erase(key.rnode_id);
        io_s.metadata_cache_replace += 1;
    }

    if(!adaptor->Remove(key.ToString())) {
        spdlog::error("delete rnode metadata rnode_id - {}: kv store interanl error", key.ToString());
        return false;
    }
    return true;
 
}


}