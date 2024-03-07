#include "wrapper/wrapper.h"

namespace wrapperfs {


struct stat* GetWrapperMetadata(std::string &value) {
    return reinterpret_cast<struct stat*> (value.data());
}


inline static void BuildLocationKey(size_t wrapper_id, wrapper_tag tag, location_key &key) {
    key.tag = tag;
    key.wrapper_id = wrapper_id;
}

inline static void BuildRelationKey(size_t wrapper_id, wrapper_tag tag, 
                                    std::string &dist, relation_key &key) {
    key.tag = tag;
    key.wrapper_id = wrapper_id;
    key.distance = dist;
}

inline static void BuildEntryKey(size_t wrapper_id, wrapper_tag tag, entry_key &key)  {
    key.wrapper_id = wrapper_id;
    key.tag = tag;
}

WrapperHandle::WrapperHandle(LevelDBAdaptor* adaptor) {
    this->adaptor = adaptor;
}

WrapperHandle::~WrapperHandle() {
    this->adaptor = nullptr;
   
    entries_cache.clear();
    location_cache.clear();
    relation_cache.clear();
}

struct stat* WrapperHandle::get_wrapper_stat(size_t wrapper_id) {

    auto ret = wrapper_cache.find(wrapper_id);
    if (ret != wrapper_cache.end()) {
        return ret->second.stat;
    }
    return nullptr;
}

// 读完需要写回缓存的噢
size_t WrapperHandle::get_relation(size_t wrapper_id, std::string filename) {

    auto ret = wrapper_cache.find(wrapper_id);
    if (ret != wrapper_cache.end()) {
         for (auto item : ret->second.relations) {
            if(item.first == filename) {
                return item.second;
            }
         }
    } else {
        return 0;
    }
}


size_t WrapperHandle::get_entry(size_t ino, std::string filename) {

    auto ret = wrapper_cache.find(ino);
    if (ret != wrapper_cache.end()) {
         for (auto item : ret->second.entries) {
            if(item.first == filename) {
                return item.second;
            }
         }
    } else {
        return 0;
    }
}

bool WrapperHandle::update_entries(entry_key &key, std::string &eval) {
    
    io_s.entry_cache_miss += 1;
    entries_cache[key.ToString()] = eval;
    
    if(wrapper_cache[key.wrapper_id].entries != nullptr) {
        delete wrapper_cache[key.wrapper_id].entries;
         wrapper_cache[key.wrapper_id].entries = nullptr;
    }
    return true;
}

bool WrapperHandle::get_entries(entry_key &key, std::string &eval) {

    io_s.entry_read += 1;
    auto ret = entries_cache.find(key.ToString());
    if (ret != entries_cache.end()) {
        io_s.entry_cache_hit += 1;
        eval = ret->second;

        if(eval.size() > 0 && wrapper_cache[key.wrapper_id].entries == nullptr) {
            // 加入wrapper_cache中
            entry_value value(eval);
            ATTR_LIST* list = new ATTR_LIST;
            value.ToList(list);
            wrapper_cache[key.wrapper_id].entries = list;
        }
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

// 用户不能主动触发put_entries，只能在缓存中的wrapper_id被location剔除才行。
bool WrapperHandle::put_entries(entry_key &key, std::string &eval) {

    if (!adaptor->Insert(key.ToString(), eval)) {
        if(ENABELD_LOG) {
            spdlog::warn("put entries tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }
    return true;
}

// 可以主动调用
bool WrapperHandle::delete_entries(entry_key &key) {

    io_s.entry_delete += 1;
    // 删除缓存
    auto ret = entries_cache.find(key.ToString());
    if (ret != entries_cache.end()) {
        entries_cache.erase(key.ToString());
        io_s.entry_cache_replace += 1;

        // 再删除二级缓存
        auto ret2 = wrapper_cache.find(key.wrapper_id);
        if(ret2 != wrapper_cache.end()) {
            if(ret2->second.entries != nullptr) {
                delete ret2->second.entries;
                ret2->second.entries = nullptr;
            }
        }
    }

    if(!adaptor->Remove(key.ToString())) {
        if(ENABELD_LOG) {
            spdlog::warn("delete entries tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }
    return true;
}



// 这个get_relation不触发二级缓存，尽量少用，且性能低下，但其保证永远都是最新的
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


// 对外触发
bool WrapperHandle::put_relation_cache(relation_key &key, size_t next_wrapper_id) {

    relation_cache.insert({key.ToString(), next_wrapper_id});
    io_s.relation_cache_replace += 1;
}


// 不对外触发，用于将二级缓存里的旧数据写回磁盘
bool WrapperHandle::put_relation() {

    for (auto item : relation_cache) {
        io_s.relation_write += 1;
        std::string rval = std::to_string(item.second);

        if (!adaptor->Insert(item.first, rval)) {
            if(ENABELD_LOG) {
                spdlog::warn("put relation tag - {} wrapper_id - {} distance - {}: kv store interanl error", key.tag, key.wrapper_id, key.distance);
            }
            continue;
        } else {
            relation_cache.erase(item.first);
        }
    }
    return true;
}

// 可主动触发
bool WrapperHandle::delete_relation(relation_key &key) {

    io_s.relation_delete += 1;

    auto ret = relation_cache.find(key.ToString());
    if (ret != relation_cache.end()) {
        relation_cache.erase(key.ToString());
        io_s.relation_cache_replace += 1;
    }

    // 将二级缓存里的情况（二级缓存是读缓存，不能写回的）
    auto ret2 = wrapper_cache.find(key.wrapper_id);
    if(ret2 != wrapper_cache.end()) {
        if(ret2->second.relations != nullptr) {
            delete ret2->second.relations;
            ret2->second.relations = nullptr;
        }
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

bool WrapperHandle::update_wrapper_cache(location_key &key, std::string &lval) {

    auto ret = wrapper_cache.find(key.ToString());
    if (ret != wrapper_cache.end()) {
        wrapper_entry entry;
        entry.stat = GetWrapperMetadata(lval);
        entry.entries = null_str;
        entry_key ekey;
        BuildEntryKey(key.wrapper_id, key.tag, ekey);
        std::string eval;
        get_entries(ekey, eval);
        wrapper_cache.insert({key.wrapper_id, entry});
    }

    if(wrapper_cache[key.wrapper_id].relations == nullptr) {
        ATTR_LIST *relation_list = new ATTR_LIST;
        relation_key rkey;
        std::string null_str("");
        BuildRelationKey(key.wrapper_id, key.tag, null_str, rkey);
        if(get_range_relations(rkey, relation_key)) {
            entry.relations = relation_list;
        } 
    }

    return true;

}

// 最主要调用的模块！
bool WrapperHandle::get_location(location_key &key, std::string &lval) {

    io_s.location_read += 1;

    auto ret = location_cache.find(key.ToString());
    if (ret != location_cache.end()) {

        io_s.location_cache_hit += 1;
        lval = ret->second;

        ATTR_LIST *relation_list = new ATTR_LIST;
        // 使用范围查询装填relation
        std::future<bool> rret = std::async(std::launch::async, &WrapperHandle::get_range_relations, this,
        std::ref(key), std::ref(relation_list));

  

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

bool WrapperHandle::clear_wrapper_cache(location_key &key) {

    auto ret = wrapper_cache.find(key.wrapper_id);
    if (ret != wrapper_cache.end()) {

        ATTR_LIST *entry_list = wrapper_cache[key.wrapper_id].entries;
        entry_value eval;
        std::string eval_str = eval.ToString(entry_list);
        entry_key ekey;
        BuildEntryKey(key.wrapper_id, key.wrapper_id, ekey);

        // 回写entries
        std::async(std::launch::async, &WrapperHandle::put_entries, this, std::ref(key), std::ref(eval_str));

        // 回写relations
        std::async(std::launch::async, &WrapperHandle::put_relation, this);

        if(wrapper_cache[key.wrapper_id].relations != nullptr) {
            delete wrapper_cache[key.wrapper_id].relations;
            wrapper_cache[key.wrapper_id].relations = nullptr;
        }

        wrapper_cache.erase(key.wrapper_id);
        location_cache.erase(key);
    }
    return true;

}

// 最频繁使用的方法，写入location的时候，所有的东西都要写回。
bool WrapperHandle::put_location(location_key &key, std::string &lval) {
    
    io_s.location_write += 1;

    if (!adaptor->Insert(key.ToString(), lval)) {
        if(ENABELD_LOG) {
            spdlog::warn("put location tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }

    clear_wrapper_cache(key);
    
    return true;
}

bool WrapperHandle::delete_location(location_key &key) {

    io_s.location_delete += 1;
    clear_wrapper_cache(key);

    if (!adaptor->Remove(key.ToString())) {
        if(ENABELD_LOG) {
            spdlog::warn("delete location tag - {} wrapper_id - {}: location doesn't exist", key.tag, key.wrapper_id);
        }
        return false;
    }
    return true;
}
}