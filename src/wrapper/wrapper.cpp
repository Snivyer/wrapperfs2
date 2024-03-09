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

void WrapperHandle::cache_entries(entry_key &key, entry_value* &eval) {
    entries_cache.insert({key.ToString(), eval});
}


bool WrapperHandle::get_entries(entry_key &key, entry_value* &eval) {

    io_s.entry_read += 1;
    auto ret = entries_cache.find(key.ToString());
    if (ret != entries_cache.end()) {
        io_s.entry_cache_hit += 1;
        eval = ret->second;
        return true;
    }

    std::string result;
    if (!adaptor->GetValue(key.ToString(), result)) {
        if(ENABELD_LOG) {
            spdlog::warn("get entries tag - {} wrapper_id - {}: entries doesn't exist", key.tag, key.wrapper_id);
        }
        eval = nullptr;
        return false;
    }

    eval = new entry_value(result);
    io_s.entry_cache_miss += 1;
    entries_cache.insert({key.ToString(), eval});
    return true;
}

// 不对外访问
bool WrapperHandle::put_entries(std::string key, std::string &eval) {

    if (!adaptor->Insert(key, eval)) {
        return false;
    }

    // 写入的时候，只需要更新一下缓存里面的标记
    auto ret = entries_cache.find(key);
    if (ret != entries_cache.end()) {
        ret->second->is_dirty = false;
    }
    return true;
}

// 已delete
bool WrapperHandle::delete_entries(entry_key &key) {

    io_s.entry_delete += 1;
    // 删除缓存
    auto ret = entries_cache.find(key.ToString());
    if (ret != entries_cache.end()) {
        delete ret->second;
        entries_cache.erase(key.ToString());
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

    auto ret1 = relation_read_only_cache.find(key.ToString());
    if (ret1 != relation_read_only_cache.end()) {
        io_s.relation_cache_hit += 1;
        next_wrapper_id = ret1->second;
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
    relation_read_only_cache[key.ToString()] = next_wrapper_id;
    return true;
}

 void WrapperHandle::cache_relation(relation_key &key, size_t &next_wrapper_id) {
    relation_cache[key.ToString()] = next_wrapper_id;
 }


bool WrapperHandle::put_relation(std::string key, size_t &next_wrapper_id) {
    
    io_s.relation_write += 1;
    std::string rval = std::to_string(next_wrapper_id);

    if (!adaptor->Insert(key, rval)) {
        return false;
    }

    relation_read_only_cache[key] = next_wrapper_id;
    return true;
}

// 已delete
bool WrapperHandle::delete_relation(relation_key &key) {

    io_s.relation_delete += 1;

    auto ret = relation_cache.find(key.ToString());
    if (ret != relation_cache.end()) {
        relation_cache.erase(key.ToString());
        return true;
    } else {

        auto ret1 = relation_read_only_cache.find(key.ToString());
        if (ret1 != relation_read_only_cache.end()) {
            relation_read_only_cache.erase(key.ToString());
        }

        if (!adaptor->Remove(key.ToString())) {
            if(ENABELD_LOG) {
                spdlog::warn("cannot delete relation");
            }
            return false;
        }
        return true;
    }
}

 bool WrapperHandle::get_range_relations(relation_key &key,  ATTR_STR_LIST* &wid2attr) {

    wid2attr->clear(); 
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
        wid2attr->emplace_back(std::pair(items[items.size() - 1], id_pair.second));
    
    }
    return true;
 }

 ATTR_STR_LIST* WrapperHandle::get_relations(relation_key &key) {

    // 将脏的relation写下去
    for(auto item: relation_cache) {
        put_relation(item.first, item.second);
    } 
    relation_cache.clear();

    ATTR_STR_LIST* id_list = new ATTR_STR_LIST;
    if(get_range_relations(key, id_list)) {
        return id_list;
    } 
    
    return nullptr;
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
    
    auto ret = location_cache.find(key.ToString());
    if (ret != location_cache.end()) {
        location_cache.erase(key.ToString());   
    }
    location_cache.insert({key.ToString(), lval}); 
    return true;
}

// 已delete
bool WrapperHandle::delete_location(location_key &key) {

    io_s.location_delete += 1;
    auto ret = location_cache.find(key.ToString());
    if (ret != location_cache.end()) {
        location_cache.erase(key.ToString());
    }

    if (!adaptor->Remove(key.ToString())) {
        if(ENABELD_LOG) {
            spdlog::warn("delete location tag - {} wrapper_id - {}: location doesn't exist", key.tag, key.wrapper_id);
        }
        return false;
    }
    return true;
}

void WrapperHandle::flush() {

    // 将脏的entries写下去
    for(auto item: entries_cache) {
        if(item.second->is_dirty == true) {
            std::string eval = item.second->ToString();
            put_entries(item.first, eval);
        }
    }

    // 将脏的relation写下去
    for(auto item: relation_cache) {
        put_relation(item.first, item.second);
    } 
    
    relation_cache.clear();

}


}