#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <utility>
#include <sys/stat.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <future>

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
    entry_value(): size(0), entry(NULL) {}
    entry_value(std::string &val) {
        size = val.size();
        entry = new char[size];
        memcpy(entry, val.data(), size);
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
        return true;
    }

    bool find(std::string str, size_t &ino) {
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


        return true;
    }

    void ToList(std::vector<std::pair<std::string, size_t>> &list) {
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
    }

    void ToList(std::vector<std::string> &list) {
        char* entry_back = entry;
        size_t count = 0;
        while(count != size) {
            size_t len = strlen(entry_back);
            std::string str = std::string(entry_back);
            list.push_back(str);
            count += len + 1 + sizeof(size_t);
            entry_back += len + 1 + sizeof(size_t);
        }
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


struct wrapper_entry {
    ATTR_LIST* entries;
    ATTR_LIST* relations;
    bool entries_is_dirty;

    wrapper_entry() {
        entries = nullptr;
        relations = nullptr;
        entries_is_dirty = false;
    }
};


class WrapperHandle {

private:
    LevelDBAdaptor* adaptor;

    std::unordered_map<std::string, size_t> relation_cache;
    std::unordered_map<std::string, std::string> location_cache;
    std::unordered_map<size_t, wrapper_entry> wrapper_cache;
    bool relation_is_dirty;
    bool relation_is_changing;

public:
    WrapperHandle(LevelDBAdaptor* adaptor);
    ~WrapperHandle();

    
    // 缓存操作
    size_t get_entry(size_t ino, std::string filename);
    bool remove_entry(size_t wrapper_id, std::string filename);
    bool put_entry(size_t wrapper_id, std::string filename, size_t ino);
    void put_relation(relation_key &key, size_t next_wrapper_id);
  

    // 磁盘操作
    bool read_entries(entry_key &key, std::string &eval);
    bool write_empty_entries(entry_key &key);
    bool delete_entries(entry_key &key);

    bool read_relation(relation_key &key, size_t &next_wrapper_id);
    bool read_cache_relation(relation_key &key, size_t &next_wrapper_id);
    bool read_range_relations(relation_key &key, ATTR_LIST &wid2attr)
    bool delete_relation(relation_key &key);


    bool read_location(location_key &key, std::string &lval);
    bool write_location(location_key &key, std::string &lval);
    bool delete_location(location_key &key);  


private:
    bool write_entries(entry_key &key, std::string &eval);
    bool write_relation(); 
    bool load_wrapper_cache(location_key &key);
    void clear_wrapper_cache(location_key &key);
};


}