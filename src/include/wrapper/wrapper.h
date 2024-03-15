#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <utility>
#include <sys/stat.h>
#include <future>
#include <spdlog/spdlog.h>
#include <unordered_map>
#include <map>


#include "wrapper/inode.h"
#include "common/config.h"
#include "adaptor/leveldb_adaptor.h"
#include "utils/string_routine.h"

namespace wrapperfs {

using ATTR_LIST = std::vector<std::pair<std::string, size_t>>;
using ATTR_STR_LIST = std::vector<std::pair<std::string, std::string>>;

static const size_t MAX_PATH_NAME = 256;

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
    std::unordered_map<std::string, size_t>* vmap;

    entry_value() {
        size = 0;
        vmap = new std::unordered_map<std::string, size_t>;
    }

    entry_value(std::string &val) {
        size = val.size();
        entry = new char[size];
        memcpy(entry, val.data(), size);
        vmap = new std::unordered_map<std::string, size_t>;
        ToMap();
    }

    ~entry_value() {
        if(entry != nullptr && size != 0) {
            delete entry;
            delete vmap;
            size = 0;
        }
    }


    void ToMap() {
    
        char* entry_back = entry;
        size_t count = 0;
        while(count != size) {
            std::string str = std::string(entry_back);
            size_t ino;
            memcpy(&ino, entry_back + MAX_PATH_NAME, sizeof(size_t));
            vmap->insert({str,ino});
            count += MAX_PATH_NAME + sizeof(size_t);
            entry_back += MAX_PATH_NAME + sizeof(size_t);
        }

    }

    std::string ToString() {

        if(size == 0) {
            return std::string(nullptr, 0);
        }

        if(size > 0) {
            delete entry;
            size = 0;
        }

        size = vmap->size() * (MAX_PATH_NAME + sizeof(size_t));
        entry = new char[size];
        char* entry_back = entry;
        for (auto item: (*vmap)) {
            memcpy(entry_back, item.first.data(), item.first.size());
            entry_back[0] = '\0';
            entry_back += MAX_PATH_NAME;
            memcpy(entry_back, &item.second, sizeof(size_t));
            entry_back += sizeof(size_t);
        }

        return std::string(entry, size);
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


struct location_buff_entry {
    location_header* lh;
    metadata_status stat;
};

struct entries_buff_entry {
    entry_value* eval;
    metadata_status stat;
};

struct relation_buff_entry {
    size_t next_wrapper_id;
    metadata_status stat;
};

class WrapperHandle {

private:
    LevelDBAdaptor* adaptor;
    std::unordered_map<std::string, entries_buff_entry> entries_buff;
    std::unordered_map<std::string, relation_buff_entry> relation_buff;
    std::unordered_map<std::string, location_buff_entry> location_buff;


    bool put_entries(std::string key);
    bool put_relation(std::string key, size_t &next_wrapper_id);
    bool put_location(std::string key);
    bool get_range_relations(relation_key &key, ATTR_STR_LIST &wid2attr);

    bool delete_location(std::string key);
    bool delete_entries(std::string key);
    bool delete_relation(std::string key);

  



public:
    WrapperHandle(LevelDBAdaptor* adaptor);
    ~WrapperHandle();

    void write_entries(std::string key, entry_value* &eval, metadata_status stat = metadata_status::write);
    bool change_entries_stat(std::string key, metadata_status state = metadata_status::write);
    bool get_entries(std::string key, entry_value* &eval);
    bool sync_entries(std::string key);
    bool sync_entrys();


    void write_relation(std::string key, size_t &next_wrapper_id, metadata_status stat = metadata_status::write);
    bool change_relation_stat(std::string key, metadata_status state = metadata_status::write);
    bool get_relation(std::string key, size_t &next_wrapper_id);
    bool get_relations(relation_key &key, ATTR_STR_LIST &list);
    bool sync_relation(std::string key);
    bool sync_relations();
    
    bool get_location(std::string key, struct location_header* &lh);
    void write_location(std::string key, struct location_header* &lh, metadata_status state = metadata_status::write);
    bool change_stat(std::string key, metadata_status state = metadata_status::write);
    bool sync_location(std::string key);
    bool sync_locations();

    bool sync();

};


}