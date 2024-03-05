#include "wrapper/inode.h"

namespace wrapperfs {



RnodeHandle::RnodeHandle(LevelDBAdaptor* adaptor) {
    this->adaptor = adaptor;
    cache.clear();
}

RnodeHandle::~RnodeHandle() {
    cache.clear();
    this->adaptor = nullptr;
}


bool RnodeHandle::get_rnode(rnode_key &key, std::string &rval) {


    io_s.metadata_read += 1;
    auto ret = cache.find(key.ToString());

    // cache hit 
    if(ret != cache.end()) {
        rval = ret->second;
        io_s.metadata_cache_hit += 1;
    } else {

        if (!adaptor->GetValue(key.ToString(), rval)) {
   
            if(ENABELD_LOG) {
                spdlog::error("get rnode metadata rnode_id - {}: rnode metadata doesn't exist", key.ToString());
            }
            return false;
        }

        io_s.metadata_cache_miss += 1;

        // 加入缓存
        cache.insert(std::unordered_map<std::string, std::string>::value_type(key.ToString(), rval));
    }
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

    auto ret = cache.find(key.ToString());

    // cache hit 
    if(ret != cache.end()) {
        cache.erase(key.ToString());
    }

    // 加入缓存
    cache.insert(std::unordered_map<std::string, std::string>::value_type(key.ToString(), rval));

    return true;

}

bool RnodeHandle::delete_rnode(rnode_key &key) {

    io_s.metadata_delete += 1;

    auto ret = cache.find(key.ToString());
    if(ret != cache.end()) {
        cache.erase(key.ToString());
    }

    if(!adaptor->Remove(key.ToString())) {
        spdlog::error("delete rnode metadata rnode_id - {}: kv store interanl error", key.ToString());
        return false;
    }
    return true;
 
}


}