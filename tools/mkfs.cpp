#include <iostream>

#include "wrapper/inode.h"
#include "wrapper/wrapper.h"
#include "adaptor/leveldb_adaptor.h"
#include "common/config.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cout << "wrapperfs mkfs error." << std::endl;
        exit(1);
    }
    wrapperfs::LevelDBAdaptor* adaptor_ = new wrapperfs::LevelDBAdaptor(argv[1]);

    wrapperfs::WrapperHandle* handle = new wrapperfs::WrapperHandle(adaptor_);


    // 创建 ROOT_WRAPPER_ID inode
    wrapperfs::location_key key;
    key.flag = 'w';
    key.tag = wrapperfs::directory_relation;
    key.wrapper_id = wrapperfs::ROOT_WRAPPER_ID;


    wrapperfs::location_header* lheader = new wrapperfs::location_header;
    lstat("./tmp", &(lheader->fstat));
    lheader->fstat.st_ino = wrapperfs::ROOT_WRAPPER_ID;
    std::string lval = std::string(reinterpret_cast<const char*>(lheader), sizeof(wrapperfs::location_header));

    if (!handle->put_location(key, lval)) {
        if (wrapperfs::ENABELD_LOG) {
            spdlog::warn("mkfs error: cannot create root wrapper!");
        }
        return false;
    }

    wrapperfs::entries_t* entries = new wrapperfs::entries_t;
    entries->wrapper_id = wrapperfs::ROOT_WRAPPER_ID;
    entries->tag = wrapperfs::directory_relation;

    if(!handle->put_entries(entries)) {
        if (wrapperfs::ENABELD_LOG) {
            spdlog::warn("mkfs error: cannot create the root wrapper entries.");
        }

        delete entries;
        return false;
    }
    
    spdlog::debug("wrapperfs mkfs success!");


    return 0;
}