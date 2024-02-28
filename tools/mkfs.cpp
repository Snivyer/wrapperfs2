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


    // 创建 ROOT_WRAPPER_ID inode
    wrapperfs::location_t* location = new wrapperfs::location_t;
    location->tag = wrapperfs::directory_relation;
    location->wrapper_id = wrapperfs::ROOT_WRAPPER_ID;
    lstat("./tmp", &(location->stat));
    location->stat.st_ino = wrapperfs::ROOT_WRAPPER_ID;


    if (!wrapperfs::put_location(adaptor_, location)) {
        if (wrapperfs::ENABELD_LOG) {
            spdlog::warn("mkfs error: cannot create root wrapper!");
        }
        delete location;
        return false;
    }

    wrapperfs::entries_t* entries = new wrapperfs::entries_t;
    entries->wrapper_id = wrapperfs::ROOT_WRAPPER_ID;
    entries->tag = wrapperfs::directory_relation;

    if(!wrapperfs::put_entries(adaptor_, entries)) {
        if (wrapperfs::ENABELD_LOG) {
            spdlog::warn("mkfs error: cannot create the root wrapper entries.");
        }

        delete entries;
        return false;
    }
    
    spdlog::debug("wrapperfs mkfs success!");


    return 0;
}