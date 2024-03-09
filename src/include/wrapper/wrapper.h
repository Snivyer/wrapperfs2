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

struct entry_key {
    wrapper_tag tag;
    size_t wrapper_id;

    std::string ToString() {
    std::stringstream s;
    s << "we" << ":" << tag << ":" << wrapper_id;
        return s.str();
    }
};

struct entry_value {
    size_t size;
    char* entry;
    bool is_dirty;

    entry_value(): size(0), entry(nullptr) {
        is_dirty = false;
    }

    entry_value(std::string &val) {
        size = val.size();
        entry = new char[size];
        memcpy(entry, val.data(), size);
        is_dirty = false;
    }

    ~entry_value() {
        if(entry != nullptr) {
            delete entry;
            size = 0;
        }
    }

    std::string ToString() {
        return std::string(entry, size);
    }

    bool push(std::string str, size_t ino) {
        size_t new_size = size + str.size() + 1 + sizeof(size_t);
        char* new_entry = new char[new_size];

        if(entry != NULL) {
            memcpy(new_entry, entry, size);
            delete entry;
        }

        memcpy(new_entry + size, str.data(), str.size());
        new_entry[size + str.size()] = '\0';
        memcpy(new_entry + size + str.size() + 1, &ino, sizeof(size_t));
        entry = new_entry;
        size = new_size;
        is_dirty = true;
        return true;
    }

    bool find(std::string str, size_t &ino) {

        if(size == 0) {
            return false;
        }

        char* entry_back = entry;
        while (strcmp(entry_back, str.data()) != 0) {
      
            size_t len = strlen(entry_back);
            entry_back += len + 1 + sizeof(size_t);

            if(entry_back >= entry + size) {
                return false;
            }
        }

        memcpy(&ino, entry_back + str.size() + 1, sizeof(size_t));
        return true;
    }

    bool remove(std::string str) {

        if(size == 0) {
            return false;
        }

        char* entry_back = entry;
        while (strcmp(entry_back, str.data()) != 0) {
      
            size_t len = strlen(entry_back);
            entry_back += len + 1 + sizeof(size_t);

            if(entry_back >= entry + size) {
                return false;
            }
        }

        size_t start = str.size() + 1 + sizeof(size_t);
        size_t skip = entry_back - entry; 
        memcpy(entry_back, entry_back + start, size - start - skip);
        size -= start;
        is_dirty = true;
        return true;
    }

    bool ToList(std::vector<std::pair<std::string, size_t>> &list) {

        if(size == 0) {
            return false;
        }

        char* entry_back = entry;
        size_t count = 0;
        while(count != size) {
            size_t len = strlen(entry_back);
            std::string str = std::string(entry_back);
            size_t ino;
            memcpy(&ino, entry_back + len + 1, sizeof(size_t));
            std::pair<std::string, size_t> p;
            p.first = str;
            p.second = ino;
            list.push_back(p);
            count += len + 1 + sizeof(size_t);
            entry_back += len + 1 + sizeof(size_t);
        }

        return true;
    }

    bool ToList(std::vector<std::string> &list) {

        if(size == 0) {
            return false;
        }

        char* entry_back = entry;
        size_t count = 0;
        while(count != size) {
            size_t len = strlen(entry_back);
            std::string str = std::string(entry_back);
            list.push_back(str);
            count += len + 1 + sizeof(size_t);
            entry_back += len + 1 + sizeof(size_t);
        }
        return true;
    }

    std::string ToString(std::vector<std::pair<std::string, size_t>> &list) {
        size_t count = 0;
        for(auto item: list) {
            count += item.first.size();
            count += 1 + sizeof(size_t);
        }

        size = count;
        entry = new char[size];
        char* entry_back = entry;
        for(auto item: list) {
            memcpy(entry_back, item.first.data(), item.first.size());
            entry_back += item.first.size();
            entry_back[0] = '\0';
            entry_back += 1;
            memcpy(entry_back, &item.second, sizeof(size_t));
            entry_back += sizeof(size_t);
        }
        return ToString();
    }
};

struct relation_key {
    wrapper_tag tag;
    size_t wrapper_id;
    std::string distance;

    std::string ToString() {
        std::stringstream s;
        s << "wr:" << tag << ":" << wrapper_id << ":" << distance;
        return s.str();
    }

    // 最后一个：不可少
    std::string ToLeftString() {
        std::stringstream s;
        s << "wr:" << tag << ":" << wrapper_id << ":";
        return s.str();
    }

    // 最后一个：必须少
    std::string ToRightString() {
        std::stringstream s;
        s << "wr:" << tag << ":" << wrapper_id + 1;
        return s.str();
    }
};

struct location_key {
    wrapper_tag tag;
    size_t wrapper_id;

    std::string ToString() {
    std::stringstream s;
    s << "wl:" << tag << ":" << wrapper_id;
        return s.str();
    }
};

struct location_header {
    struct stat fstat;
};


class WrapperHandle {

private:
    LevelDBAdaptor* adaptor;
    std::unordered_map<std::string, entry_value*> entries_cache;
    std::unordered_map<std::string, size_t> relation_cache;
    std::unordered_map<std::string, size_t> relation_read_only_cache;
    std::unordered_map<std::string, std::string> location_cache;


    bool put_entries(std::string key, std::string &eval);
    bool put_relation(std::string key, size_t &next_wrapper_id);
    bool get_range_relations(relation_key &key, ATTR_STR_LIST* &wid2attr);
 


public:
    WrapperHandle(LevelDBAdaptor* adaptor);
    ~WrapperHandle();

    void cache_entries(entry_key &key, entry_value* &eval);
    bool get_entries(entry_key &key, entry_value* &eval);
    bool delete_entries(entry_key &key);

    void cache_relation(relation_key &key, size_t &next_wrapper_id);
    bool get_relation(relation_key &key, size_t &next_wrapper_id);
    bool delete_relation(relation_key &key);
    ATTR_STR_LIST* get_relations(relation_key &key);
    
    bool get_location(location_key &key, std::string &lval);
    bool put_location(location_key &key, std::string &lval);
    bool delete_location(location_key &key);   

    void flush();

};


}