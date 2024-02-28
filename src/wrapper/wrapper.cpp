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

std::pair<std::string, std::string> decode_range_relotions(wrapper_tag tag, size_t wrapper_id) {
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


// bug: 这里的entries重新申请了， 会不会delete错，需要自己delete (solved)
bool get_entries(LevelDBAdaptor* adaptor, entries_t* &entries) {
    std::string key = decode_entries(entries);
    std::string value;

    if (!adaptor->GetValue(key, value)) {
        delete entries;
        spdlog::warn("get entries tag - {} wrapper_id - {}: entries doesn't exist", entries->tag, entries->wrapper_id);
        return false;
    }

    delete entries;
    entries = new entries_t;
    try {
        nlohmann::json json = nlohmann::json::parse(value);
        json.at("tag").get_to(entries->tag);
        json.at("wrapper_id").get_to(entries->wrapper_id);
        json.at("list").get_to(entries->list);
    } catch (const std::exception& e) {
        delete entries;
        spdlog::warn("get entries tag - {} wrapper_id - {}: unresolved data format", entries->tag, entries->wrapper_id);
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("get entries: {}", entries->debug());
    }
    return true;
}

// 已delete
    bool put_entries(LevelDBAdaptor* adaptor, entries_t* entries) {
    if (entries == nullptr) {
        spdlog::warn("put entries: entries doesn't exist");
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
        delete entries;
        spdlog::warn("put entries tag - {} wrapper_id - {}: kv store interanl error", entries->tag, entries->wrapper_id);
        exit(1);
    }
    delete entries;
    return true;
}

// 已delete
bool delete_entries(LevelDBAdaptor* adaptor, entries_t* entries) {

    if (entries == nullptr) {
        spdlog::warn("delete entries: entries doesn't exist");
        exit(1);
    }
    std::string key = decode_entries(entries);

    if(!adaptor->Remove(key)) {
        delete entries;
        spdlog::warn("delete entries tag - {} wrapper_id - {}: kv store interanl error", entries->tag, entries->wrapper_id);
        exit(1);
    }

    delete entries;
    return true;
}

bool get_relation(LevelDBAdaptor* adaptor, relation_t* &relation) {
    std::string key = decode_relation(relation);
    std::string value;

    if (!adaptor->GetValue(key, value)) {
        spdlog::warn("cannot get relation");
        return false;
    }

    try {
        nlohmann::json json = nlohmann::json::parse(value);
        json.at("tag").get_to(relation->tag);
        json.at("wrapper_id").get_to(relation->wrapper_id);
        json.at("distance").get_to(relation->distance);
        json.at("next_wrapper_id").get_to(relation->next_wrapper_id);
    } catch (const std::exception& e) {
        relation = nullptr;
        spdlog::warn("get relation tag - {} wrapper_id - {} distance - {}: unresolved data format", relation->tag, relation->wrapper_id, relation->distance);
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("get relation: {}", relation->debug());
    }
    return true;
}


// 已delete
bool put_relation(LevelDBAdaptor* adaptor, relation_t* relation) {
    if (relation == nullptr) {
        spdlog::warn("put relation: relation doesn't exist");
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
        delete relation;
        spdlog::warn("put relation tag - {} wrapper_id - {} distance - {}: kv store interanl error", relation->tag, relation->wrapper_id, relation->distance);
        exit(1);
    }
    delete relation;
    return true;
}

// 已delete
bool delete_relation(LevelDBAdaptor* adaptor, relation_t* relation) {

    std::string key = decode_relation(relation);

    if (!adaptor->Remove(key)) {
        spdlog::warn("cannot delete relation");
        delete relation;
        return false;
    }

    delete relation;
    return true;
}



bool get_location(LevelDBAdaptor* adaptor, location_t* &location) {
    std::string key = decode_location(location);
    std::string value;
    if (!adaptor->GetValue(key, value)) {
        spdlog::warn("get location tag - {} wrapper_id - {}: location doesn't exist", location->tag, location->wrapper_id);
        delete location;
        return false;
    }

    try {
        const struct stat* stat = reinterpret_cast<const struct stat *>(value.data());
        std::memcpy(&location->stat, stat, sizeof(struct stat)); 

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
bool put_location(LevelDBAdaptor* adaptor, location_t* location) {
    if (location == nullptr) {
        spdlog::warn("put location: location doesn't exist");
        exit(1);
    }
    if (ENABELD_LOG) {
        spdlog::info("put location: {}", location->debug());
    }
    std::string key = decode_location(location);
    std::string stat_value = std::string(reinterpret_cast<const char*>(&location->stat), sizeof(struct stat));
    if (!adaptor->Insert(key, stat_value)) {
        spdlog::warn("put location tag - {} wrapper_id - {}: kv store interanl error", location->tag, location->wrapper_id);
        delete location;
        exit(1);
    }
    delete location;
    return true;
}

// 已delete
bool delete_location(LevelDBAdaptor* adaptor, location_t* location) {

    std::string key = decode_location(location);

    if (!adaptor->Remove(key)) {
        spdlog::warn("delete location tag - {} wrapper_id - {}: location doesn't exist", location->tag, location->wrapper_id);
        delete location;
        return false;
    }

    delete location;
    return true;

}

bool get_range_relations(LevelDBAdaptor* adaptor, wrapper_tag tag, size_t wrapper_id, std::vector<relation_t> &relations) {
    relations.clear();
    std::pair<std::string, std::string> keys = decode_range_relotions(tag, wrapper_id);
    std::vector<std::pair<std::string, std::string>> key_value_pair_list;
    if (!adaptor->GetRange(keys.first, keys.second, key_value_pair_list)) {
        spdlog::warn("get range relations tag - {} wrapper_id - {}: kv store interanl error", tag, wrapper_id);
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
            spdlog::warn("get range relations tag - {} wrapper_id - {}: unresolved data format", tag, wrapper_id);
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