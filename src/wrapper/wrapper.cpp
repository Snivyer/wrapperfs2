#include "wrapper/wrapper.h"

namespace wrapperfs {


struct stat* GetMetadata(std::string &value) {
    return reinterpret_cast<struct stat*> (value.data());
}


WrapperHandle::WrapperHandle(LevelDBAdaptor* adaptor) {
    this->adaptor = adaptor;
}

WrapperHandle::~WrapperHandle() {
    this->adaptor = nullptr;
    sync();
    entries_buff.clear();
    location_buff.clear();
    relation_buff.clear();
}

void WrapperHandle::write_entries(std::string key, entry_value* &eval, metadata_status state) {
    entries_buff[key].eval = eval;
    entries_buff[key].stat = state;
}

void WrapperHandle::change_entries_stat(std::string key, metadata_status state) {
    entries_buff[key].stat = state;
}


bool WrapperHandle::get_entries(std::string key, entry_value* &eval) {

    io_s.entry_read += 1;
    auto ret = entries_buff.find(key);
    if (ret != entries_buff.end()) {
        io_s.entry_cache_hit += 1;
        eval = ret->second.eval;

        if(ret->second.stat != metadata_status::remove) {
            return true;
        } else {
            return false;
        }
    }

    std::string result;
    if (!adaptor->GetValue(key, result)) {
        eval = nullptr;
        return false;
    }

    eval = new entry_value(result);
    io_s.entry_cache_miss += 1;
    write_entries(key, eval, metadata_status::read);
    return true;
}

// 不对外访问
bool WrapperHandle::put_entries(std::string key) {

    io_s.entry_write += 1;

    entry_value* eval = entries_buff[key].eval;
    std::string eval_str = eval->ToString();

    if (!adaptor->Insert(key, eval_str)) {
        return false;
    }
    change_entries_stat(key, metadata_status::read);
    return true;
}

// 已delete
bool WrapperHandle::delete_entries(std::string key) {

    io_s.entry_delete += 1;
    // 删除缓存

    if(entries_buff[key].eval) {
        delete entries_buff[key].eval->entry;
        delete entries_buff[key].eval;
    }

    entries_buff.erase(key);
    if(!adaptor->Remove(key)) {
        return false;
    }
    return true;
}

bool WrapperHandle::sync_entries(std::string key) {

    if( entries_buff[key].stat == metadata_status::write) {
        return put_entries(key);
    }

    if( entries_buff[key].stat == metadata_status::remove) {
        return delete_entries(key);
    }
    return true;
}

void WrapperHandle::write_relation(std::string key, size_t &next_wrapper_id, metadata_status state) {
    relation_buff[key].next_wrapper_id = next_wrapper_id;
    relation_buff[key].stat = state;
}

void  WrapperHandle::change_relation_stat(std::string key, metadata_status state) {
    relation_buff[key].stat = state;
}

bool WrapperHandle::get_relation(std::string key, size_t &next_wrapper_id) {
 
    io_s.relation_read += 1;
    auto ret = relation_buff.find(key);
    if (ret != relation_buff.end()) {
        io_s.relation_cache_hit += 1;
        next_wrapper_id = ret->second.next_wrapper_id;
        if(ret->second.stat != metadata_status::remove) {
            return true;
        } else {
            return false;
        }
    } 

    io_s.relation_cache_miss += 1;
    std::string rval;
    if (!adaptor->GetValue(key, rval)) {

        if(ENABELD_LOG) {
            spdlog::warn("cannot get relation");
        }
        return false;
    }
    next_wrapper_id = std::stoi(rval);
    write_relation(key, next_wrapper_id, metadata_status::read);
    return true;
}


bool WrapperHandle::put_relation(std::string key, size_t &next_wrapper_id) {
   
    io_s.relation_write += 1;
    std::string rval = std::to_string(next_wrapper_id);
    if (!adaptor->Insert(key, rval)) {
        return false;
    }
    change_relation_stat(key, metadata_status::read);
    return true;
}

// 已delete
bool WrapperHandle::delete_relation(std::string key) {

    io_s.relation_delete += 1;

    relation_buff.erase(key);
    if (!adaptor->Remove(key)) {
        if(ENABELD_LOG) {
            spdlog::warn("cannot delete relation");
        }
        return false;
    }
    return true;
}

bool WrapperHandle::get_range_relations(relation_key &key,  ATTR_STR_LIST &wid2attr) {

    wid2attr.clear(); 
    io_s.relation_range_read += 1;

    std::string leftkey = key.ToLeftString();
    std::string rightkey = key.ToRightString();

    if (!adaptor->GetRange(leftkey, rightkey, wid2attr)) {
        if(ENABELD_LOG) {
            spdlog::warn("get range relations tag - {} wrapper_id - {}: kv store interanl error", key.tag, key.wrapper_id);
        }
        return false;
    }

    for (auto item: wid2attr) {
        relation_buff[item.first].next_wrapper_id = std::stoi(item.second);
        relation_buff[item.first].stat = metadata_status::read;
    }
    return true;
 }


bool WrapperHandle::get_relations(relation_key &key, ATTR_STR_LIST &list) {

    // 首先第一步，先落盘
    for(auto item: relation_buff) {
        if(item.second.stat > metadata_status::read) {
            sync_relation(item.first);
        }
    }

    if(get_range_relations(key, list)) {
        return true;
    } 
    return false;
 }


bool WrapperHandle::sync_relation(std::string key) {
    
    if(relation_buff[key].stat == metadata_status::write) {
        return put_relation(key, relation_buff[key].next_wrapper_id);
    }

    if(relation_buff[key].stat == metadata_status::remove) {
        return delete_relation(key);
    }
    return true;
}


void WrapperHandle::change_stat(std::string key, metadata_status state) {
    location_buff[key].stat = state;
}

void WrapperHandle::write_location(std::string key, struct location_header* &lh, metadata_status state) {
    location_buff[key].lh = lh;
    location_buff[key].stat = state;
}
  
// bug: 不需要delete了，因为已经缓存了
bool WrapperHandle::get_location(std::string key, struct location_header* &lh) {

    io_s.location_read += 1;
    auto ret = location_buff.find(key);
    if(ret != location_buff.end()) {
        io_s.metadata_cache_hit += 1;
        lh = ret->second.lh;
        if(ret->second.stat != metadata_status::remove) {
            return true;
        } else {
            return false;
        }
    }

    std::string lval;
    if (!adaptor->GetValue(key, lval)) {
        return false;
    }

    lh = new location_header;
    memcpy(&(lh->fstat), GetMetadata(lval), sizeof(struct stat));
    io_s.location_cache_miss += 1;
    write_location(key, lh, metadata_status::read);
    return true;
}


bool WrapperHandle::put_location(std::string key) {
    
    io_s.location_write += 1;

    location_header* header = location_buff[key].lh;
    std::string lval =  std::string(reinterpret_cast<const char*>(header), sizeof(location_header));
    if (!adaptor->Insert(key, lval)) {
        return false;
    }
    
    change_stat(key, metadata_status::read);
    return true;
}

// 已delete
bool WrapperHandle::delete_location(std::string key) {

    io_s.location_delete += 1;

    if(location_buff[key].lh) {
        delete location_buff[key].lh;
    }
    location_buff.erase(key);
    if (!adaptor->Remove(key)) {
        return false;
    }
    return true;
}

bool WrapperHandle::sync_location(std::string key) {

    if(location_buff[key].stat == metadata_status::write) {
        return put_location(key);
    }

    if(location_buff[key].stat == metadata_status::remove) {
        return delete_location(key);
    }
    return true;
}

bool WrapperHandle::sync() {

    for(auto item: relation_buff) {
        if(item.second.stat > metadata_status::read) {
            sync_relation(item.first);
        }
    }

    for(auto item: entries_buff) {
        if(item.second.stat > metadata_status::read) {
            sync_entries(item.first);
        }
    }

    for(auto item: location_buff) {
        if(item.second.stat > metadata_status::read) {
            sync_location(item.first);
        }
    }

}



}