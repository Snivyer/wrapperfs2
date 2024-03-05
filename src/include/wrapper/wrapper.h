#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <utility>
#include <sys/stat.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <unordered_map>

#include "common/config.h"
#include "adaptor/leveldb_adaptor.h"
#include "utils/string_routine.h"

namespace wrapperfs {

using ATTR_LIST = std::vector<std::pair<std::string, size_t>>;
using ATTR_STR_LIST = std::vector<std::pair<std::string, std::string>>;

enum wrapper_tag {
    directory_relation,
};

// 点查询
struct entries_t {
    wrapper_tag tag;
    size_t wrapper_id;
    std::vector<std::pair<size_t, std::string>> list;

    std::string debug() {
        std::stringstream s;
        s << "tag:" << tag;
        s << "\t";
        s << " wrapper_id:" << wrapper_id;
        for (auto &entry: list) {
            s << " ino - " << entry.first;
            s << " attr - " << entry.second;
        }
        return s.str();
    }
};


struct relation_key {
    char flag;
    wrapper_tag tag;
    size_t wrapper_id;
    std::string distance;

    std::string ToString() {
        std::stringstream s;
        s << flag << ":" << tag << ":" << wrapper_id << ":" << distance;
        return s.str();
    }

    // 最后一个：不可少
    std::string ToLeftString() {
        std::stringstream s;
        s << flag << ":" << tag << ":" << wrapper_id << ":";
        return s.str();
    }

    // 最后一个：必须少
    std::string ToRightString() {
        std::stringstream s;
        s << flag << ":" << tag << ":" << wrapper_id + 1;
        return s.str();
    }
};



struct location_key {
    char flag;
    wrapper_tag tag;
    size_t wrapper_id;

    std::string ToString() {
    std::stringstream s;
    s << flag << ":" << tag << ":" << wrapper_id;
        return s.str();
    }
};

struct location_header {
    struct stat fstat;
};





class WrapperHandle {

private:
    LevelDBAdaptor* adaptor;
    std::unordered_map<std::string, entries_t*> entries_cache;
    std::unordered_map<std::string, size_t> relation_cache;
    std::unordered_map<std::string, std::string> location_cache;


public:
    WrapperHandle(LevelDBAdaptor* adaptor);
    ~WrapperHandle();

    bool get_entries(entries_t* &entries);
    bool put_entries(entries_t* entries);
    bool delete_entries(entries_t* entries);

    bool get_relation(relation_key &key, size_t &next_wrapper_id);
    bool put_relation(relation_key &key, size_t &next_wrapper_id);
    bool delete_relation(relation_key &key);
    bool get_range_relations(relation_key &key, ATTR_LIST &wid2attr);
    


    bool get_location(location_key &key, std::string &lval);
    bool put_location(location_key &key, std::string &lval);
    bool delete_location(location_key &key);   







};


}