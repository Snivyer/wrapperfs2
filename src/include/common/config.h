#pragma once

#include <spdlog/spdlog.h>
#include <sstream>

namespace wrapperfs {



struct Operation_Statistic {
    size_t getattr;
    size_t getFileStat;
    size_t getDirStat;
    size_t getNULLStat;
    size_t mknod;
    size_t mkdir;
    size_t unlink;
    size_t rmdir;
    size_t rename;
    size_t chmod;
    size_t chown;
    size_t open;
    size_t read;
    size_t write;
    size_t release;
    size_t opendir;
    size_t readdir;
    size_t releasedir;
    size_t access;
    size_t utimens;

    Operation_Statistic() {
        getattr  = 0;
        getFileStat = 0;
        getDirStat = 0;
        getNULLStat = 0;
        mknod = 0;
        unlink = 0;
        rmdir = 0;
        rename = 0;
        chmod = 0;
        chown = 0;
        open = 0;
        read = 0;
        write = 0;
        release = 0;
        opendir = 0;
        readdir = 0;
        releasedir  = 0;
        access = 0;
        utimens = 0;
    }

    std::string debug() {
        std::stringstream s;
        s << "getattr:" << getattr;
        s << "\t";
        s << "getFileStat:" << getFileStat;
        s << "\t";
        s << "getDirStat:" << getDirStat;
        s << "\t";
        s << "getNULLStat:" << getNULLStat;
        s << "\t";
        s << "mknod:" << mknod;
        s << "\t";
        s << "unlink:" << unlink;
        s << "\t";
        s << "rmdir:" << rmdir;
        s << "\t";
        s << "rename:" << rename;
        s << "\t";
        s << "chmod:" << chmod;
        s << "\t";
        s << "chown:" << chown;
        s << "\t";       
        s << "open:" << open;
        s << "\t";  
        s << "read:" << read;
        s << "\t";
        s << "write:" << write;
        s << "\t";
        s << "release:" << release;
        s << "\t";
        s << "opendir:" << opendir;
        s << "\t";
        s << "readdir:" << readdir;
        s << "\t";
        s << "releasedir:" << releasedir;
        s << "\t";
        s << "access:" << access;
        s << "\t";
        s << "utimens:" << utimens;
        return s.str();
    } 
};

struct IO_Statistic {
    size_t relation_read;
    size_t relattion_write;
    size_t relation_delete;
    size_t relation_range_read;
    size_t location_read;
    size_t location_write;
    size_t location_delete;
    size_t metadata_read;
    size_t metadata_write;
    size_t metadata_delete;
    size_t entry_read;
    size_t entry_write;
    size_t entry_delete;

    IO_Statistic() {
        relation_read = 0;
        relattion_write = 0;
        relation_delete = 0;
        relation_range_read = 0;
        location_read = 0;
        location_write = 0;
        location_delete = 0;
        metadata_read = 0;
        metadata_write = 0;
        metadata_delete = 0;
        entry_read = 0;
        entry_write = 0;
        entry_delete = 0;
    }


    std::string debug() {
        std::stringstream s;
        s << "relation_read:" << relation_read;
        s << "\t";
        s << "relation_write:" << relattion_write;
        s << "\t";
        s << "relation_delete:" << relation_delete;
        s << "\t";
        s << "relation_range_read:" << relation_range_read;
        s << "\t";
        s << "location_read:" << location_read;
        s << "\t";
        s << "location_write:" << location_write;
        s << "\t";
        s << "location_delete:" << location_delete;
        s << "\t";
        s << "metadata_read:" << metadata_read;
        s << "\t";       
        s << "metadata_write:" << metadata_write;
        s << "\t";  
        s << "metadata_delete:" << metadata_delete;
        s << "\t";
        s << "entry_read:" << entry_read;
        s << "\t";
        s << "entry_write:" << entry_write;
        s << "\t";
        s << "entry_delete:" << entry_delete;
        return s.str();
    }


};


extern bool ENABELD_LOG;
extern bool STATISTICS_LOG;
extern spdlog::level::level_enum DEFAULT_LOG_LEVEL;
extern size_t ROOT_WRAPPER_ID;
extern char* PATH_DELIMITER;
extern size_t NUM_FILES_IN_DATADIR;
extern size_t NUM_FILES_IN_DATADIR_BITS;
extern struct Operation_Statistic op_s;
extern struct IO_Statistic io_s;

}