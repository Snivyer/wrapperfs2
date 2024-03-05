 #pragma once

#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <sstream>
#include <spdlog/spdlog.h>

#include "wrapper/inode.h"
#include "wrapper/wrapper.h"
#include "utils/hash_routine.h"
#include "utils/string_routine.h"
#include "utils/filesystem_routine.h"
#include "adaptor/leveldb_adaptor.h"
#include "file_handle.h"

namespace wrapperfs {

class wrapperfs {
public:
    wrapperfs(const std::string &data_dir, const std::string &db_dir);
    int Getattr(const char* path, struct stat* statbuf);
    int Mknod(const char* path, mode_t mode, dev_t dev);
    int Mkdir(const char* path, mode_t mode);
    int Open(const char* path, struct fuse_file_info* file_info);
    int Read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* file_info);
    int Write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* file_info);
    int Release(const char* path, struct fuse_file_info* file_info);
    int Opendir(const char* path, struct fuse_file_info* file_info);
    int Readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* file_info);
    int Releasedir(const char* path, struct fuse_file_info* file_info);
    int Access(const char* path, int mask);
    int UpdateTimes(const char* path, const struct timespec tv[2]);
    int RemoveDir(const char *path);
    int Unlink(const char *path);
    int Chmod(const char *path, mode_t mode);
    int Chown(const char *path, uid_t uid, gid_t gid);
    int Rename(const char* source, const char* dest);
    void Destroy(void *data);

private:
    size_t max_ino;
    size_t max_wrapper_id;
    std::string data_dir_;
    LevelDBAdaptor* adaptor_;
    RnodeHandle* rnode_handle;
    WrapperHandle* wrapper_handle;


    void GetFilePath(size_t ino, std::string &path);
    bool PathLookup(const char* path, size_t &wrapper_id, bool &is_file, std::string &filename);
    bool PathLookup(const char* path, size_t &wrapper_id, bool &is_file, std::string &filename, size_t &pc_id);
    bool PathResolution(std::vector<std::string> &path_items, size_t &wrapper_id_in_search);
    bool WrapperLookup(size_t &wrapper_id, size_t &next_wrapper_id, std::string &distance);
    bool EntriesLookup(size_t &wrapper_id, size_t &ino, std::string &primary_attr);
    bool GetFileStat(size_t &ino, struct stat *stat);
    bool GetWrapperStat(size_t wrapper_id, struct stat *stat);
    void InitStat(struct stat &stat, size_t ino, mode_t mode, dev_t dev);
    bool UpdateWrapperMetadata(struct stat &stat, size_t wrapper_id);
    bool UpdateMetadata(mode_t mode, dev_t dev, size_t ino);
    bool UpdateMetadata(struct stat &stat, size_t ino);






    
};

}