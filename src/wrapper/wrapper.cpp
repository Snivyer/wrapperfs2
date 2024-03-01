#include "wrapper/wrapper.h"

namespace wrapperfs {

std::string decode_entries(entries_t* entries) {
    std::ostringstream oss;
    oss << "entries:";
    switch (entries->tag) {
        case directory_relation:
            oss << "d:";
            break;
    }
    oss << std::setw(6) << std::setfill('0') << entries->wrapper_id;
    return oss.str();
}

std::string decode_relation(relation_t* relation) {
    std::ostringstream oss;
    oss << "relations:";
    switch (relation->tag) {
        case directory_relation:
            oss << "d:";
            break;
    }
    oss << std::setw(6) << std::setfill('0') << relation->wrapper_id << ":";
    oss << relation->distance;
    return oss.str();
}

std::string decode_location(location_t* location) {
    std::ostringstream oss;
    oss << "location:";
    switch (location->tag) {
        case directory_relation:
            oss << "d:";
            break;
    }
    oss << std::setw(6) << std::setfill('0') << location->wrapper_id << ":";
    return oss.str();
}

std::pair<std::string, std::string> decode_range_relations(wrapper_tag tag, size_t wrapper_id) {
    std::ostringstream start_key_oss;
    std::ostringstream end_key_oss;
    start_key_oss << "relations:";
    end_key_oss << "relations:";
    switch (tag) {
        case directory_relation:
            start_key_oss << "d:";
            end_key_oss << "d:";
            break;
    }
    start_key_oss << std::setw(6) << std::setfill('0') << wrapper_id << ":";
    end_key_oss << std::setw(6) << std::setfill('0') << wrapper_id + 1 << ":";
    return std::make_pair(start_key_oss.str(), end_key_oss.str());
}


WrapperHandle::WrapperHandle(LevelDBAdaptor* adaptor) {
    this->adaptor = adaptor;
}

WrapperHandle::~WrapperHandle() {
    this->adaptor = nullptr;
   
    for(auto item:entries_cache) {
        
        if(item.second != nullptr) {
            delete item.second;
        }
    }
    entries_cache.clear();

    for(auto item:location_cache) {
        
        if(item.second != nullptr) {
            delete item.second;
        }
    }
    location_cache.clear();


    for(auto item:relation_cache) {
        
        if(item.second != nullptr) {
            delete item.second;
        }
    }
    relation_cache.clear();
}


// bug: 这里的entries重新申请了， 会不会delete错，需要自己delete (solved)
// bug: 不需要delete了，因为已经缓存了
bool WrapperHandle::get_entries(entries_t* &entries) {
    std::string key = decode_entries(entries);
    std::string value;

    io_s.entry_read += 1;

    auto ret = entries_cache.find(key);
    if (ret != entries_cache.end()) {

        // 先删除外面构造的
        if (entries!= nullptr) {
            delete entries;
        }
        io_s.entry_cache_hit += 1;
        entries = ret->second;
        return true;
    }

    io_s.entry_cache_miss += 1;
    if (!adaptor->GetValue(key, value)) {
        delete entries;
        if(ENABELD_LOG) {
            spdlog::warn("get entries tag - {} wrapper_id - {}: entries doesn't exist", entries->tag, entries->wrapper_id);
        }
        return false;
    }

    try {
        nlohmann::json json = nlohmann::json::parse(value);
        json.at("tag").get_to(entries->tag);
        json.at("wrapper_id").get_to(entries->wrapper_id);
        json.at("list").get_to(entries->list);
        entries_cache.insert(std::unordered_map<std::string, entries_t*>::value_type(key, entries));
    } catch (const std::exception& e) {
        delete entries;
        entries = nullptr;
        if(ENABELD_LOG) {
            spdlog::warn("get entries tag - {} wrapper_id - {}: unresolved data format", entries->tag, entries->wrapper_id);
        }
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("get entries: {}", entries->debug());
    }
    return true;
}

// 已delete
bool WrapperHandle::put_entries(entries_t* entries) {

    io_s.entry_write += 1;
        
    if (entries == nullptr) {
        if(ENABELD_LOG) {
            spdlog::warn("put entries: entries doesn't exist");
        }
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("put entries: {}", entries->debug());
    }
    std::string key = decode_entries(entries);
    nlohmann::json json = nlohmann::json{{"tag", entries->tag},
                                          {"wrapper_id", entries->wrapper_id},
                                          {"list", entries->list}};
    std::string value = json.dump();
    if (!adaptor->Insert(key, value)) {
        if(ENABELD_LOG) {
            spdlog::warn("put entries tag - {} wrapper_id - {}: kv store interanl error", entries->tag, entries->wrapper_id);
        }
        delete entries;
        entries = nullptr;
        exit(1);
    }

    // 写入的时候，先把缓存里的旧的删除，然后加入新的
    auto ret = entries_cache.find(key);
    if (ret != entries_cache.end()) {
        entries_cache.erase(key);
    }
    entries_cache.insert(std::unordered_map<std::string, entries_t*>::value_type(key, entries));
    return true;
}

// 已delete
bool WrapperHandle::delete_entries(entries_t* entries) {

    io_s.entry_delete += 1;

    if (entries == nullptr) {
        if(ENABELD_LOG) {
            spdlog::warn("delete entries: entries doesn't exist");
        }
        exit(1);
    }
    std::string key = decode_entries(entries);
        // 删除缓存
    auto ret = entries_cache.find(key);
    if (ret != entries_cache.end()) {
        entries_cache.erase(key);
    }

    if(!adaptor->Remove(key)) {
        delete entries;
        if(ENABELD_LOG) {
            spdlog::warn("delete entries tag - {} wrapper_id - {}: kv store interanl error", entries->tag, entries->wrapper_id);
        }
        exit(1);
    }

    delete entries;
    entries = nullptr;
    return true;
}

// bug: 不需要delete了，因为已经缓存了
bool WrapperHandle::get_relation(relation_t* &relation) {
    std::string key = decode_relation(relation);
    std::string value;
    io_s.relation_read += 1;

    auto ret = relation_cache.find(key);
    if (ret != relation_cache.end()) {

        // 先删除外面构造的
        if (relation!= nullptr) {
            delete relation;
        }
        io_s.relation_cache_hit += 1;
        relation = ret->second;
        return true;
    } 

    io_s.relation_cache_miss += 1;
    if (!adaptor->GetValue(key, value)) {

        if(ENABELD_LOG) {
            spdlog::warn("cannot get relation");
        }
        delete relation;
        return false;
    }

    try {
        nlohmann::json json = nlohmann::json::parse(value);
        json.at("tag").get_to(relation->tag);
        json.at("wrapper_id").get_to(relation->wrapper_id);
        json.at("distance").get_to(relation->distance);
        json.at("next_wrapper_id").get_to(relation->next_wrapper_id);
        relation_cache.insert(std::unordered_map<std::string, relation_t*>::value_type(key, relation));

    } catch (const std::exception& e) {
        delete relation;
        relation = nullptr;

        if(ENABELD_LOG) {
            spdlog::warn("get relation tag - {} wrapper_id - {} distance - {}: unresolved data format", relation->tag, relation->wrapper_id, relation->distance);
        }
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("get relation: {}", relation->debug());
    }
    return true;
}


// 已delete
bool WrapperHandle::put_relation(relation_t* relation) {
    io_s.relation_write += 1;

    if (relation == nullptr) {

        if(ENABELD_LOG) {
            spdlog::warn("put relation: relation doesn't exist");
        }
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("put relation: {}", relation->debug());
    }
    std::string key = decode_relation(relation);

    nlohmann::json json = nlohmann::json{{"tag", relation->tag},
                                          {"wrapper_id", relation->wrapper_id},
                                          {"distance", relation->distance},
                                          {"next_wrapper_id", relation->next_wrapper_id}};
    std::string value = json.dump();
    if (!adaptor->Insert(key, value)) {
        if(ENABELD_LOG) {
            spdlog::warn("put relation tag - {} wrapper_id - {} distance - {}: kv store interanl error", relation->tag, relation->wrapper_id, relation->distance);
        }
        delete relation;
        relation = nullptr;
        exit(1);
    }

    auto ret = relation_cache.find(key);
    if (ret != relation_cache.end()) {
        relation_cache.erase(key);
    }
    relation_cache.insert(std::unordered_map<std::string, relation_t*>::value_type(key, relation));
    return true;
}

// 已delete
bool WrapperHandle::delete_relation(relation_t* relation) {

 
    io_s.relation_delete += 1;
    std::string key = decode_relation(relation);

    auto ret = relation_cache.find(key);
    if (ret != relation_cache.end()) {
        relation_cache.erase(key);
    }

    if (!adaptor->Remove(key)) {
        if(ENABELD_LOG) {
            spdlog::warn("cannot delete relation");
        }
        delete relation;
        relation = nullptr;
        return false;
    }

    delete relation;
    return true;
}

// bug: 不需要delete了，因为已经缓存了
bool WrapperHandle::get_location(location_t* &location) {
    io_s.location_read += 1;
    std::string key = decode_location(location);
    std::string value;

    auto ret = location_cache.find(key);
    if (ret != location_cache.end()) {

         // 先删除外面构造的
        if (location!= nullptr) {
            delete location;
        }

        io_s.location_cache_hit += 1;
        location = ret->second;
        return true;
    } 

    io_s.location_cache_miss += 1;
    if (!adaptor->GetValue(key, value)) {
        if(ENABELD_LOG) {
            spdlog::warn("get location tag - {} wrapper_id - {}: location doesn't exist", location->tag, location->wrapper_id);
        }
        delete location;
        location = nullptr;
        return false;
    }

    try {
        const struct stat* stat = reinterpret_cast<const struct stat *>(value.data());
        std::memcpy(&location->stat, stat, sizeof(struct stat));
        location_cache.insert(std::unordered_map<std::string, location_t*>::value_type(key, location));


    } catch (const std::exception& e) {
        spdlog::warn("get location tag - {} wrapper_id - {}: unresolved data format", location->tag, location->wrapper_id);
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("get location: {}", location->debug());
    }
    return true;
}

// 已delete
bool WrapperHandle::put_location(location_t* location) {
    io_s.location_write += 1;

    if (location == nullptr) {
        if(ENABELD_LOG) {
            spdlog::warn("put location: location doesn't exist");
        }
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("put location: {}", location->debug());
    }
    std::string key = decode_location(location);
    std::string stat_value = std::string(reinterpret_cast<const char*>(&location->stat), sizeof(struct stat));
    if (!adaptor->Insert(key, stat_value)) {
        if(ENABELD_LOG) {
            spdlog::warn("put location tag - {} wrapper_id - {}: kv store interanl error", location->tag, location->wrapper_id);
        }
        delete location;
        location = nullptr;
        exit(1);
    }
    
    auto ret = location_cache.find(key);
    if (ret != location_cache.end()) {
        location_cache.erase(key);   
    }
    location_cache.insert(std::unordered_map<std::string, location_t*>::value_type(key, location));
    return true;
}

// 已delete
bool WrapperHandle::delete_location(location_t* location) {


    io_s.location_delete += 1;
    std::string key = decode_location(location);

    auto ret = location_cache.find(key);
    if (ret != location_cache.end()) {
        location_cache.erase(key);
    }

    if (!adaptor->Remove(key)) {
        if(ENABELD_LOG) {
            spdlog::warn("delete location tag - {} wrapper_id - {}: location doesn't exist", location->tag, location->wrapper_id);
        }
        delete location;
        location = nullptr;
        return false;
    }
    
    delete location;
    location = nullptr;
    return true;

}


// FIXME: 这里的地址我没有管
bool WrapperHandle::get_range_relations(wrapper_tag tag, size_t wrapper_id, std::vector<relation_t> &relations) {
    
    io_s.relation_range_read += 1;

    relations.clear();
    std::pair<std::string, std::string> keys = decode_range_relations(tag, wrapper_id);
    std::vector<std::pair<std::string, std::string>> key_value_pair_list;
    if (!adaptor->GetRange(keys.first, keys.second, key_value_pair_list)) {
        if(ENABELD_LOG) {
            spdlog::warn("get range relations tag - {} wrapper_id - {}: kv store interanl error", tag, wrapper_id);
        }
        exit(1);
    }
    for (auto &key_value_pair : key_value_pair_list) {
        relation_t relation;
        try {
            nlohmann::json json = nlohmann::json::parse(key_value_pair.second);
            json.at("tag").get_to(relation.tag);
            json.at("wrapper_id").get_to(relation.wrapper_id);
            json.at("distance").get_to(relation.distance);
            json.at("next_wrapper_id").get_to(relation.next_wrapper_id);
        } catch (const std::exception& e) {
            if(ENABELD_LOG) {
                spdlog::warn("get range relations tag - {} wrapper_id - {}: unresolved data format", tag, wrapper_id);
            }
            exit(1);
        }
        if (ENABELD_LOG) {
            spdlog::info("get range relations: {}", relation.debug());
        }
        relations.emplace_back(relation);
    }
    return true;
}

}