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
    key.tag = wrapperfs::directory_relation;
    key.wrapper_id = wrapperfs::ROOT_WRAPPER_ID;


    wrapperfs::location_header* lheader = new wrapperfs::location_header;
    lstat("./tmp", &(lheader->fstat));
    lheader->fstat.st_ino = wrapperfs::ROOT_WRAPPER_ID;
    std::string lval = std::string(reinterpret_cast<const char*>(lheader), sizeof(wrapperfs::location_header));

    handle->write_location(key.ToString(), lheader);
    handle->sync_location(key.ToString());
    
    wrapperfs::entry_key ekey;
    ekey.wrapper_id = wrapperfs::ROOT_WRAPPER_ID;
    ekey.tag = wrapperfs::directory_relation; 

    wrapperfs::entry_value* eval = new wrapperfs::entry_value;
    handle->write_entries(ekey.ToString(), eval);
    handle->sync_entries(ekey.ToString());
    
    spdlog::debug("wrapperfs mkfs success!");
    return 0;
}