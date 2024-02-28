#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <utility>
#include <sys/stat.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "common/config.h"
#include "adaptor/leveldb_adaptor.h"

namespace wrapperfs {

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

// 范围查询
struct relation_t {
    wrapper_tag tag;
    size_t wrapper_id;
    std::string distance;
    size_t next_wrapper_id;

    std::string debug() {
        std::stringstream s;
        s << "tag - " << tag;
        s << " wid" << wrapper_id;
        s << " dist - " << distance;
        s << " next wid - " << next_wrapper_id;
        return s.str();
    }
};

// 点查询
struct location_t {
    wrapper_tag tag;
    size_t wrapper_id;
    struct stat stat;

    std::string debug() {
        std::stringstream s;
        s << "tag:" << tag;
        s << "\t";
        s << " wid:" << wrapper_id;
        return s.str();
    }
};

bool get_entries(LevelDBAdaptor* adaptor, entries_t* &entries);
bool put_entries(LevelDBAdaptor* adaptor, entries_t* entries);
bool delete_entries(LevelDBAdaptor* adaptor, entries_t* entries);
bool get_relation(LevelDBAdaptor* adaptor, relation_t* &relation);
bool put_relation(LevelDBAdaptor* adaptor, relation_t* relation);
bool delete_relation(LevelDBAdaptor* adaptor, relation_t* relation);
bool get_location(LevelDBAdaptor* adaptor, location_t* &location);
bool put_location(LevelDBAdaptor* adaptor, location_t* location);
bool delete_location(LevelDBAdaptor* adaptor, location_t* location);
bool get_range_relations(LevelDBAdaptor* adaptor, wrapper_tag tag, size_t wrapper_id, std::vector<relation_t> &relations);

}