#include "wrapper/wrapper.h"

namespace wrapperfs {


WrapperHandle::WrapperHandle(LevelDBAdaptor* adaptor) {
    this->adaptor = adaptor;
}

WrapperHandle::~WrapperHandle() {
    this->adaptor = nullptr;
   
    entries_cache.clear();
    location_cache.clear();
    relation_cache.clear();
}

bool WrapperHandle::get_entries(entry_key &key, std::string &eval) {

    io_s.entry_read += 1;
    auto ret = entries_cache.find(key.ToString());
    if (ret != entries_cache.end()) {
        io_s.entry_cache_hit += 1;
        eval = ret->second;
        return true;
    }

    if (!adaptor->GetValue(key.ToString(), eval)) {
        if(ENABELD_LOG) {
            spdlog::warn("get entries tag - {} wrapper_id - {}: entries doesn't exist", key.tag, key.wrapper_id);
        }
        return false;
    }

    io_s.entry_cache_miss += 1;
    entries_cache.insert({key.ToString(), eval});
    io_s.entry_cache_replace += 1;
    return true;
}

// 已delete
bool WrapperHandle::put_entries(entry_key &key, std::string &eval) {

    if (!adaptor->Insert(key.ToString(), eval)) {
        if(ENABELD_LOG) {
            spdlog::warn("put entries tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }

    // 写入的时候，先把缓存里的旧的删除，然后加入新的
    entries_cache[key.ToString()] = eval;
    io_s.entry_cache_replace += 1;
   

    return true;
}

// 已delete
bool WrapperHandle::delete_entries(entry_key &key) {

    io_s.entry_delete += 1;
    // 删除缓存
    auto ret = entries_cache.find(key.ToString());
    if (ret != entries_cache.end()) {
        entries_cache.erase(key.ToString());
        io_s.entry_cache_replace += 1;
    }

    if(!adaptor->Remove(key.ToString())) {
        if(ENABELD_LOG) {
            spdlog::warn("delete entries tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }
    return true;
}


bool WrapperHandle::get_relation(relation_key &key, size_t &next_wrapper_id) {
 
    std::string rval;
    io_s.relation_read += 1;

    auto ret = relation_cache.find(key.ToString());
    if (ret != relation_cache.end()) {

        io_s.relation_cache_hit += 1;
        next_wrapper_id = ret->second;
        return true;
    } 

    io_s.relation_cache_miss += 1;
    if (!adaptor->GetValue(key.ToString(), rval)) {

        if(ENABELD_LOG) {
            spdlog::warn("cannot get relation");
        }
        return false;
    }
    next_wrapper_id = std::stoi(rval);
    relation_cache.insert({key.ToString(), next_wrapper_id});
    io_s.relation_cache_replace += 1;
    return true;
}



bool WrapperHandle::put_relation(relation_key &key, size_t &next_wrapper_id) {
    
    io_s.relation_write += 1;
    std::string rval = std::to_string(next_wrapper_id);
    
    if (!adaptor->Insert(key.ToString(), rval)) {
        if(ENABELD_LOG) {
            spdlog::warn("put relation tag - {} wrapper_id - {} distance - {}: kv store interanl error", key.tag, key.wrapper_id, key.distance);
        }
     
        return false;
    }

    relation_cache[key.ToString()] = next_wrapper_id;
    io_s.relation_cache_replace += 1;
    return true;
}

// 已delete
bool WrapperHandle::delete_relation(relation_key &key) {

    io_s.relation_delete += 1;

    auto ret = relation_cache.find(key.ToString());
    if (ret != relation_cache.end()) {
        relation_cache.erase(key.ToString());
        io_s.relation_cache_replace += 1;
    }

    if (!adaptor->Remove(key.ToString())) {
        if(ENABELD_LOG) {
            spdlog::warn("cannot delete relation");
        }
        return false;
    }

    return true;
}

 bool WrapperHandle::get_range_relations(relation_key &key, ATTR_LIST &wid2attr) {
    
    io_s.relation_range_read += 1;

    std::string leftkey = key.ToLeftString();
    std::string rightkey = key.ToRightString();

    ATTR_STR_LIST id_list;

    if (!adaptor->GetRange(leftkey, rightkey, id_list)) {
        if(ENABELD_LOG) {
            spdlog::warn("get range relations tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }

    for (auto &id_pair : id_list) {
        // 可以分割出文件名
        std::vector<std::string> items = split_string(id_pair.first, ":");
        wid2attr.emplace_back(std::pair(items[items.size() - 1], std::stoi(id_pair.second)));
    
    }
    return true;
 }

// bug: 不需要delete了，因为已经缓存了
bool WrapperHandle::get_location(location_key &key, std::string &lval) {

    io_s.location_read += 1;

    auto ret = location_cache.find(key.ToString());
    if (ret != location_cache.end()) {
        io_s.location_cache_hit += 1;
        lval = ret->second;
        return true;
    } 

    if (!adaptor->GetValue(key.ToString(), lval)) {
        if(ENABELD_LOG) {
            spdlog::warn("get location tag - {} wrapper_id - {}: location doesn't exist", key.tag, key.wrapper_id);
        }
        return false;
    }

    io_s.location_cache_miss += 1;
    location_cache.insert({key.ToString(), lval});
    io_s.location_cache_replace += 1;
    return true;
}

// 已delete
bool WrapperHandle::put_location(location_key &key, std::string &lval) {
    
    io_s.location_write += 1;

    if (!adaptor->Insert(key.ToString(), lval)) {
        if(ENABELD_LOG) {
            spdlog::warn("put location tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }
    
    location_cache[key.ToString()] = lval;
    io_s.location_cache_replace += 1;
    return true;
}

// 已delete
bool WrapperHandle::delete_location(location_key &key) {

    io_s.location_delete += 1;
    auto ret = location_cache.find(key.ToString());
    if (ret != location_cache.end()) {
        location_cache.erase(key.ToString());
        io_s.location_cache_replace += 1;
    }

    if (!adaptor->Remove(key.ToString())) {
        if(ENABELD_LOG) {
            spdlog::warn("delete location tag - {} wrapper_id - {}: location doesn't exist", key.tag, key.wrapper_id);
        }
        return false;
    }
    return true;
}
}