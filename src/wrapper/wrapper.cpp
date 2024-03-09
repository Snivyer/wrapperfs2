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
    relation_is_dirty = false;
    relation_is_changing = false;
}

WrapperHandle::~WrapperHandle() {
    this->adaptor = nullptr;
    wrapper_cache.clear();
    location_cache.clear();
    relation_cache.clear();
}


// 从wrapper缓存中拿最新的entries
size_t WrapperHandle::get_entry(size_t wrapper_id, std::string filename) {

    auto ret = wrapper_cache.find(wrapper_id);
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

// 删除成功，需要标记entries变脏，以写回去
bool WrapperHandle::remove_entry(size_t wrapper_id, std::string filename) {

    ATTR_LIST* list = wrapper_cache[wrapper_id].entries;
    if(list != nullptr) { 
        auto it = list->begin();
        while (it != list->end()) {
            if (filename == (*it).first) {
                list->erase(it);
                wrapper_cache[wrapper_id].entries_is_dirty = true;
            } else {
                it++;
            }
        }
    }
    return false;
}


// 添加新entries以后，也需要标记entry变脏，以写回去
bool WrapperHandle::put_entry(size_t wrapper_id, std::string filename, size_t ino) {
    ATTR_LIST* list = wrapper_cache[wrapper_id].entries;
    if(list != nullptr) { 
       list->push_back(std::pair(filename, ino));
       wrapper_cache[wrapper_id].entries_is_dirty = true;
       return true;
    }
    return false;
}


// 只有当缓存读不到的时候，才会使用
bool WrapperHandle::read_entries(entry_key &key, std::string &eval) {

    io_s.entry_read += 1;
    io_s.entry_cache_miss += 1;

    if (!adaptor->GetValue(key.ToString(), eval)) {
        if(ENABELD_LOG) {
            spdlog::warn("get entries tag - {} wrapper_id - {}: entries doesn't exist", key.tag, key.wrapper_id);
        }
        return false;
    } 

    auto ret = wrapper_cache.find(key.wrapper_id);
    if (ret != wrapper_cache.end()) {
        ATTR_LIST* list = new ATTR_LIST;
        entry_value val(eval);
        val.ToList(list);
        wrapper_cache[key.wrapper_id].entries = list;
        io_s.entry_cache_replace += 1;

    }
    return true;
}




bool WrapperHandle::write_empty_entries(entry_key &key) {
    std::string nullstr("");
    return write_entries(key, nullptr);
}



// 用户不能主动触发put_entries，只能在缓存中的wrapper_id被location剔除才行。
bool WrapperHandle::write_entries(entry_key &key, std::string &eval) {

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

    // 删除缓存，因为缓存项没有用了，反正entries要变空了
    auto ret = wrapper_cache.find(key.wrapper_id);
    if(ret != wrapper_cache.end()) {
        if(ret->second.entries != nullptr) {
            delete ret2->second.entries;
            ret->second.entries = nullptr;
            ret->second.entries_is_dirty = false;
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



// 这个read_relation不触发二级缓存，尽量少用，且性能低下，但其保证永远都是最新的
bool WrapperHandle::read_relation(relation_key &key, size_t &next_wrapper_id) {
 
    std::string rval;
    io_s.relation_read += 1;

    // 先查读写缓存
    auto ret = relation_cache.find(key.ToString());
    if (ret != relation_cache.end()) {
        io_s.relation_cache_hit += 1;
        next_wrapper_id = ret->second;
        return true;
    } 

    // 再查范围查询缓存
    auto ret = wrapper_cache.find(key.wrapper_id);
    if (ret != wrapper_cache.end() && relation_is_changing == false) {
         for (auto item : ret->second.relations) {
            if(item.first == key.distance) {
                next_wrapper_id = item.second;
                return true;
            }
         }
    } 

    io_s.relation_cache_miss += 1;
    if (!adaptor->GetValue(key.ToString(), rval)) {

        if(ENABELD_LOG) {
            spdlog::warn("cannot get relation");
        }
        return false;
    }

    next_wrapper_id = std::stoi(rval);
    return true;
}

bool WrapperHandle::read_cache_relation(relation_key &key, size_t &next_wrapper_id) {

    io_s.relation_read += 1;

    // 先查读写缓存
    auto ret = relation_cache.find(key.ToString());
    if (ret != relation_cache.end()) {
        io_s.relation_cache_hit += 1;
        next_wrapper_id = ret->second;
        return true;
    } 

    auto ret = wrapper_cache.find(key.wrapper_id);
    if (ret != wrapper_cache.end()) {

        if(wrapper_cache[key.wrapper_id].relations != nullptr) {
            delete wrapper_cache[key.wrapper_id].relations;
        }
    
        ATTR_LIST *rlist = new ATTR_LIST;
        relation_key rkey;
        std::string null_str("");
        BuildRelationKey(key.wrapper_id, key.tag, null_str, rkey);
        read_range_relations(rkey, rlist);
        wrapper_cache[key.wrapper_id].relations = rlist;
        relation_is_changing == false;
    
        for (auto item : ret->second.relations) {
            if(item.first == key.distance) {
                next_wrapper_id = item.second;
                return true;
            }
        }

    io_s.relation_cache_miss += 1;
  }
}





// 对外触发
void WrapperHandle::put_relation(relation_key &key, size_t next_wrapper_id) {

    relation_cache.insert({key.ToString(), next_wrapper_id});
    io_s.relation_cache_replace += 1;
    relation_is_dirty = true;
}


// 不对外触发，用于将二级缓存里的旧数据写回磁盘
bool WrapperHandle::write_relation() {

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
    relation_is_dirty = false;
    relation_is_changing = true;
    return true;
}

// 可主动触发
bool WrapperHandle::delete_relation(relation_key &key) {

    io_s.relation_delete += 1;

    auto ret = relation_cache.find(key.ToString());
    if (ret != relation_cache.end()) {
        relation_cache.erase(key.ToString());
        io_s.relation_cache_replace += 1;
    } else {

        if (!adaptor->Remove(key.ToString())) {
            if(ENABELD_LOG) {
                spdlog::warn("cannot delete relation");
            }
            return false;
        }
        relation_is_changing = true;
    }
    return true;
}

 bool WrapperHandle::read_range_relations(relation_key &key, ATTR_LIST &wid2attr) {
    
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

void WrapperHandle::load_wrapper_cache(location_key &key) {

    auto ret = wrapper_cache.find(key.wrapper_id);
    if (ret == wrapper_cache.end()) {
        wrapper_entry entry;
        wrapper_cache.insert({key.wrapper_id, entry});
    } 
 
    // 加载最新的entries
    if (wrapper_cache[key.wrapper_id].entries == nullptr) {
        entry_key ekey;
        BuildEntryKey(key.wrapper_id, key.tag, ekey);
        std::string eval;
        read_entries(ekey, eval); 
        wrapper_cache[key.wrapper_id].entries_is_dirty = false;
    }

    //  改动就需要加载
    if(relation_is_changing == true) {

        if(wrapper_cache[key.wrapper_id].relations != nullptr) {
            delete wrapper_cache[key.wrapper_id].relations;
        }

        ATTR_LIST *rlist = new ATTR_LIST;
        relation_key rkey;
        std::string null_str("");
        BuildRelationKey(key.wrapper_id, key.tag, null_str, rkey);
        read_range_relations(rkey, rlist);
        wrapper_cache[key.wrapper_id].relations = rlist;
        relation_is_nochange = false;
    }
}

// 最主要调用的模块！
bool WrapperHandle::read_location(location_key &key, std::string &lval) {

    io_s.location_read += 1;

    auto ret = location_cache.find(key.ToString());
    if (ret != location_cache.end()) {
        io_s.location_cache_hit += 1;
        lval = ret->second;
        std::async(std::launch::async, &WrapperHandle::load_wrapper_cache, this, std::ref(key));
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


void WrapperHandle::clear_wrapper_cache(location_key &key) {


    auto ret = wrapper_cache.find(key.wrapper_id);
    if (ret != wrapper_cache.end()) {

        // 清空写缓存
        write_relation();

        // 清空读缓存
        if(wrapper_cache[key.wrapper_id].relations != nullptr) {
            delete wrapper_cache[key.wrapper_id].relations;
        }

        // 清空写缓存
        if(wrapper_cache[key.wrapper_id].entries_is_dirty == true) {
            ATTR_LIST *entry_list = wrapper_cache[key.wrapper_id].entries;
            entry_value eval;
            std::string eval_str = eval.ToString(entry_list);
            entry_key ekey;
            BuildEntryKey(key.tag, key.wrapper_id, ekey);
            write_entries(key, eval_str);
            delete entry_list;
            wrapper_cache[key.wrapper_id].entries_is_dirty = false;

        }

        wrapper_cache.erase(key.wrapper_id);
    }
}

// 最频繁使用的方法，写入location的时候，所有的东西都要写回。
bool WrapperHandle::write_location(location_key &key, std::string &lval) {
    
    io_s.location_write += 1;

    if (!adaptor->Insert(key.ToString(), lval)) {
        if(ENABELD_LOG) {
            spdlog::warn("put location tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }
    std::async(std::launch::async, &WrapperHandle::clear_wrapper_cache, this, std::ref(key));

    location_cache[key.ToString] = lval;
    return true;
}

bool WrapperHandle::delete_location(location_key &key) {

    io_s.location_delete += 1;
    auto ret = location_cache.find(key.ToString());
    if (ret != location_cache.end()) {
        location_cache.erase(key.ToString());
        io_s.location_cache_replace += 1;
        std::async(std::launch::async, &WrapperHandle::clear_wrapper_cache, this, std::ref(key));
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