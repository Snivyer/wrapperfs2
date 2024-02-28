#include "fs/wrapperfs.h"

namespace wrapperfs {

// wrapperfs的初始化过程
// 初始化的关键参数有：max_ino, max_wrapper_id
wrapperfs::wrapperfs(const std::string &data_dir, const std::string &db_dir) {
    max_ino = 0;
    max_wrapper_id = ROOT_WRAPPER_ID;
    data_dir_ = data_dir;
    adaptor_ = new LevelDBAdaptor(db_dir);
}

bool wrapperfs::PathResolution(std::vector<std::string> &path_items, size_t &wrapper_id_in_search) {

    int index = -1;
    while (index != path_items.size() - 2) {
        
        relation_t* relation = new relation_t;
        relation->tag = directory_relation;
        relation->wrapper_id = wrapper_id_in_search;
        relation->distance = path_items[index + 1];

        if (!get_relation(adaptor_, relation)) {
            break;
        } else {
                index += 1;
                wrapper_id_in_search  = relation->next_wrapper_id;
        }
        delete relation;
    }

    if (index != path_items.size() - 2) {
        if (ENABELD_LOG) {
            spdlog::warn("path resolution failed");
        }
        return -ENOENT;
    }
}

 bool wrapperfs::WrapperLookup(size_t &wrapper_id, size_t &next_wrapper_id, std::string &distance) {

     // 首先判断是否能够找到目录
    relation_t* relation = new relation_t;
    relation->tag = directory_relation;
    relation->wrapper_id = wrapper_id;
    relation->distance = distance;

    // 如果能够找到关系，那么就是目录
    if (get_relation(adaptor_, relation) ) {
        next_wrapper_id = relation->next_wrapper_id;
        delete relation;
        return true;
    }

    delete relation;
    return false;
 }

 bool wrapperfs::EntriesLookup(size_t &wrapper_id, size_t &ino, std::string &primary_attr) {

    entries_t* entries = new entries_t; 
    entries->tag = directory_relation;
    entries->wrapper_id = wrapper_id;

    // 如果能够找到，那么就是文件
    if (get_entries(adaptor_, entries)) {
        for (auto &entry : entries->list) {
            if (primary_attr == entry.second) {
                ino = entry.first;
                delete entries;
                return true;
            }
        }
        delete entries;
    } 
    return false;
 }

 

// bug: 无法区分目录或文件是新目录，还是旧目录 (solved: 新目录或新文件将返回false)
bool wrapperfs::PathLookup(const char* path, size_t &wrapper_id, bool &is_file, std::string &filename) {

    std::string path_string = path;

    if (path_string.empty() || path_string.front() != *PATH_DELIMITER) {
        if (ENABELD_LOG) {
            spdlog::warn("wrapper lookup failed");
        }
        return false;
    }

    std::vector<std::string> path_items = split_string(path_string, PATH_DELIMITER);
    size_t wrapper_id_in_search = ROOT_WRAPPER_ID;

    // 解析根目录和包装器标签目录
    if (path_items.size() < 2) {

        // 为根目录
        if (path_items.size() == 0) {
            is_file = false;
            wrapper_id = ROOT_WRAPPER_ID;
            return true;
        } 

    } else {
        PathResolution(path_items, wrapper_id_in_search);
    }

    filename = path_items[path_items.size() - 1];
    size_t ino;

    if (WrapperLookup(wrapper_id_in_search, wrapper_id, filename)) {
        is_file = false;
        return true;
    }

    if (EntriesLookup(wrapper_id_in_search, ino, filename)) {
        is_file = true;
        wrapper_id = wrapper_id_in_search;
        return true;
    }
 
     return false;
}

// bug: 在get_entries过程中出现了段错误，原因出自于delete metadata，在这个过程中metadata的地址发生了几次变化，导致delete不掉了 (solved)
// bug: 在get_entries过程中出现了段错误 (solved 查找成功可以delete，查找失败则不能)
bool wrapperfs::GetFileStat(size_t wrapper_id, std::string filename, size_t &ino, struct stat *stat) {

    entries_t* entries = new entries_t;
    entries->tag = directory_relation;
    entries->wrapper_id = wrapper_id;

    if (!get_entries(adaptor_, entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("getStat error, single file");
        }
        return false;
    } else {
        for (auto &entry : entries->list) {
            if (filename == entry.second) {
                ino = entry.first;
                inode_metadata_t* metadata = new inode_metadata_t;

                if (!get_inode_metadata(adaptor_, entry.first, metadata)) {
                    if (ENABELD_LOG) {
                        spdlog::warn("getStat error, single file");
                    }
                    delete entries;
                    delete metadata;
                    return false;
                } else {
                    std::memcpy(stat, &metadata->stat, sizeof(struct stat));
                    delete entries;
                    delete metadata;
                    return true;
                }
            }
        }

        if (ENABELD_LOG) {
            spdlog::warn("getStat error, single file");
        }
        delete entries;
        return false;
    }
}

bool wrapperfs::GetWrapperStat(size_t wrapper_id, struct stat *stat) {
    
    location_t* location = new location_t;
    location->tag = directory_relation;
    location->wrapper_id = wrapper_id;
    if (!get_location(adaptor_, location)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr: return failed");
        }
        delete location;
        return false;
    } else {
        std::memcpy(stat, &location->stat, sizeof(struct stat));
        delete location;
        return true;
    }  
}

bool wrapperfs::UpdateMetadata(struct stat &stat, size_t ino) {
    
    inode_metadata_t *metadata = new inode_metadata_t;
    std::memcpy(&(metadata->stat), &stat, sizeof(struct stat));
        
    if (!put_inode_metadata(adaptor_, ino, metadata)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: put file stat error");
        }
        delete metadata;
        return -ENONET;
    }

}

bool wrapperfs::UpdateWrapperMetadata(struct stat &stat, size_t wrapper_id) {

    // 首先创建location，将wrapper写进去
    location_t* locate = new location_t;
    locate->wrapper_id = wrapper_id;
    locate->tag = directory_relation;
    std::memcpy(&(locate->stat), &stat, sizeof(struct stat));

    if(!put_location(adaptor_, locate)) {
        if (ENABELD_LOG) {
            spdlog::warn("mkdir error: cannot create directory wrapper.");
        }
        delete locate; 
        return -ENONET;
    }

}

// bug: 现有的PathLookup，无法获取新目录或新文件的元数据 (solved)
// bug: 多层目录的访问还存在bug，问题出现在路径解析方法中 (solved: 创建目录时没有创建entries)
 int wrapperfs::Getattr(const char* path, struct stat* statbuf) {

    bool is_file;
    std::string filename;
    size_t wrapper_id;
    size_t ino;

    if(!PathLookup(path, wrapper_id, is_file, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
         return -ENOENT;
    }

    if (is_file == true)   {
        if(!GetFileStat(wrapper_id, filename, ino, statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get file stat error");
            }
            return -ENOENT;
        }  
    } else {
        if(!GetWrapperStat(wrapper_id, statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get wrapper stat error");
            }
            return -ENOENT; 
        } 
    }

    return 0;
}

// bug: 自己构建的stat，总是不合法，总是报错，我试试下面的方法，Input/output error （solved）
// 方法一：直接使用根目录的stat，结果：可行
// 方法二：采用lstat来填充stat，然后将其余的信息补充，结果：不行
// 方法三：直接采用lstat来填充，不补充信息，结果：可行
// 那么问题出现在我填充的某个字段上，通过不断的测试，发现st_mode字段出现了问题。
// 问题分析：难道是因为mount目录的权限是774，普通用户无法在该目录下创建目录 (权限修改成777也不行)
// 通过阅读tablefs的代码，发现在mkdir时候需要使用mode|S_IFDIR来表明它是目录，在mknod使用mode|S_IFREG表明其是文件
void wrapperfs::InitStat(struct stat &stat, size_t ino, mode_t mode, dev_t dev) {
    
    stat.st_ino = ino;
    stat.st_mode = mode;
    stat.st_dev = dev;

    stat.st_gid = fuse_get_context()->gid;
    stat.st_uid = fuse_get_context()->uid;
  
    stat.st_size = 0;
    stat.st_blksize = 0;
    stat.st_blocks = 0;

    if S_ISREG(mode) {
        stat.st_nlink = 1;
    } else {
        stat.st_nlink = 2;
    }

    time_t now = time(NULL);
    stat.st_atim.tv_sec = now; 
    stat.st_atim.tv_nsec = 0;
    stat.st_mtim.tv_sec = now;
    stat.st_mtim.tv_nsec = 0;
    stat.st_ctim.tv_sec = now;
    stat.st_ctim.tv_nsec = 0;
}

// bug: 文件创建以后，居然显示not file，这里面肯定有逻辑错误 （solved）
// bug: 获取不到父目录的entries (sovled: 因为传错了一个wrapper id)
int wrapperfs::Mknod(const char* path, mode_t mode, dev_t dev) {

    std::string path_string = path;
    size_t wrapper_id = ROOT_WRAPPER_ID;
    std::vector<std::string> path_items = split_string(path_string, PATH_DELIMITER);
    std::string filename = path_items[path_items.size() - 1];

    if (path_items.size() > 1 ) {
        PathResolution(path_items, wrapper_id);
    }
  
    // 将文件元数据写进去
    max_ino = max_ino + 1;
    size_t ino = max_ino;

    struct stat stat;

    // FIXME: mode需要加入S_IFREG，以标注是文件
    InitStat(stat, ino, mode | S_IFREG, dev);

    UpdateMetadata(stat, ino);

    // 还需要将文件名作为额外元数据写进去
    entries_t* entries = new entries_t;
    entries->wrapper_id = wrapper_id;
    entries->tag = directory_relation;
        
    if (!get_entries(adaptor_, entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: cannot get name metadata.");
        }
        return -ENONET;
    } 
        
    entries->list.push_back(std::pair(ino, filename)); 
    if(!put_entries(adaptor_, entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: cannot put name metadata into db.");
        }
        delete entries;
        return -ENONET;
    }

    return 0;
}

// FIXME: 这里在创建目录stat的时候，采用的是wrapper_id作为inode id传进去的，不知道有没有问题
int wrapperfs::Mkdir(const char* path, mode_t mode) {

    size_t wrapper_id = ROOT_WRAPPER_ID;
    std::vector<std::string> path_items = split_string(path, PATH_DELIMITER);
    std::string filename = path_items[path_items.size() - 1];

    if (path_items.size() > 1 ) {
        PathResolution(path_items, wrapper_id);
    }
    
    max_wrapper_id = max_wrapper_id + 1;
    size_t create_wrapper_id = max_wrapper_id;
    struct stat stat;
    // FIXME: 需要加上S_IFDIR，以标记是目录
    InitStat(stat, create_wrapper_id, mode | S_IFDIR, 0);

    // 首先创建location，将wrapper写进去
    location_t* locate = new location_t;
    locate->wrapper_id = create_wrapper_id;
    locate->tag = directory_relation;
    std::memcpy(&(locate->stat), &stat, sizeof(struct stat));

    if(!put_location(adaptor_, locate)) {
        if (ENABELD_LOG) {
            spdlog::warn("mkdir error: cannot create directory wrapper.");
        }
        delete locate; 
        return -ENONET;
    }

    // 还需要将目录关系写进去
    relation_t* relation = new relation_t;
    relation->tag = directory_relation;
    relation->wrapper_id = wrapper_id;
    relation->distance = filename;
    relation->next_wrapper_id = create_wrapper_id;

    if(!put_relation(adaptor_, relation))  {
             
        if (ENABELD_LOG) {
            spdlog::warn("mkdir error: cannot create the relation of dirctory wrapper.");
        }
        delete relation; 
        return -ENONET;
    }

    // 最后将空entries写进去
    entries_t* entries = new entries_t;
    entries->wrapper_id = create_wrapper_id;
    entries->tag = directory_relation;

    if(!put_entries(adaptor_, entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("mkfs error: cannot create empty entries.");
        }

        delete entries;
        return -ENONET;
    }
    return 0;
}

//
int wrapperfs::Open(const char* path, struct fuse_file_info* file_info) {

    std::string filename;
    size_t ino;
    file_handle_t* fh = new file_handle_t;

    size_t wrapper_id = ROOT_WRAPPER_ID;
    std::vector<std::string> path_items = split_string(path, PATH_DELIMITER);
    filename = path_items[path_items.size() - 1];

    if (path_items.size() > 1 ) {
        PathResolution(path_items, wrapper_id);
    }

    if(!EntriesLookup(wrapper_id, ino, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
        return -ENOENT;
    }

    if(!GetFileStat(wrapper_id, filename, ino, &(fh->stat))) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot get the stat");
        }
        return -ENOENT;
    }

    fh->ino = ino;
    fh->flags = file_info->flags;
    std::string real_path;
    GetFilePath(ino, real_path);

    fh->fd = open(real_path.c_str(), file_info->flags | O_CREAT, fh->stat.st_mode);

    if(fh->fd < 0) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot open the real file");
        }
        return -ENOENT;
    }

    file_info->fh = (uint64_t)fh;
    return 0;
}


// bug: 为什么数据读不出来呀，明明已经读到了，但是没办法显示 (solved)
// 办法1：先直接读取一个固定的文件数据，看看能否读取出来 结果：还是获得不了数据
// 办法2：查资料发现：在进行read操作之前，FUSE会先调用getattr()，然后用文件的size属性阶段read操作的结果
 int wrapperfs::Read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* file_info) {

    std::string path_string = path;
    file_handle_t* fh = reinterpret_cast<file_handle_t*> (file_info->fh);

    if (fh->fd < 0) { 
        GetFilePath(fh->ino, path_string);
        fh->fd = open(path_string.c_str(), fh->flags | O_CREAT, fh->stat.st_mode);

    }
    int ret;
    if (fh->fd >= 0) {
        ret = pread(fh->fd, buf, size, offset);
    } else {
        if (ENABELD_LOG) {
            spdlog::warn("read error: cannot read file");
        }
        ret = -ENOENT;
    }

    return ret;
}

int wrapperfs::Write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* file_info) {

   std::string path_string = path;
   file_handle_t* fh = reinterpret_cast<file_handle_t*> (file_info->fh);

    if (fh->fd < 0) {
        GetFilePath(fh->ino, path_string);
        fh->fd = open(path_string.c_str(), fh->flags | O_CREAT, fh->stat.st_mode);
    }

    int ret;
    if (fh->fd >= 0) {
        ret = pwrite(fh->fd, buf, size, offset);
    } else {
        if (ENABELD_LOG) {
            spdlog::warn("read error: cannot wirte file");
        }
        ret = -ENOENT;
    }

    // FIXME: 写完数据以后，记得及时更新文件大小
    if(ret > 0) {
        fh->stat.st_size = offset + size;
          
        // 将更新好的数据写回DB
        UpdateMetadata(fh->stat, fh->ino);
    }
    return ret;
}

int wrapperfs::Unlink(const char *path) {

    std::string filename;
    size_t ino;
    bool is_remove = false;
    size_t wrapper_id = ROOT_WRAPPER_ID;

    std::vector<std::string> path_items = split_string(path, PATH_DELIMITER);
    filename = path_items[path_items.size() - 1];

    if (path_items.size() > 1 ) {
        PathResolution(path_items, wrapper_id);
    }

    if(!EntriesLookup(wrapper_id, ino, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
        return -ENOENT;
    }

    entries_t* entries = new entries_t;
    entries->tag = directory_relation;
    entries->wrapper_id = wrapper_id;

    // FIXME: 这里的get_entries可以进一步省掉
    if (!get_entries(adaptor_, entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("unlink error, cannot get entries");
        }
        return -ENOENT;
    } else {
        auto it = entries->list.begin();
        while (it != entries->list.end()) {
            if (filename == (*it).second) {

                // 删除元数据
                if(!delete_inode_metadata(adaptor_, (*it).first)) {
                    if (ENABELD_LOG) {
                        spdlog::warn("delete file error");
                    }
                    delete entries;
                    return -ENOENT;
                }

                // 移除entry
                entries->list.erase(it);
                is_remove = true;
                break;
            } else {
                it++;
            }
               
        }
    }

    if (is_remove) {
        if (!put_entries(adaptor_, entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("delete file error, can not put deleted entries");
            }
            delete entries;
            return -ENOENT;
        }
        return 0;
    } else {

        if (ENABELD_LOG) {
            spdlog::warn("delete file error, can not find the deleted file.");
        }
        delete entries;
        return -ENOENT;
    }
}

//
int wrapperfs::Release(const char* path, struct fuse_file_info* file_info) {

    file_handle_t* fh = reinterpret_cast<file_handle_t*> (file_info->fh);

    fh->stat.st_atim.tv_sec  = time(NULL);
    fh->stat.st_atim.tv_nsec = 0;
    fh->stat.st_mtim.tv_sec  = time(NULL);
    fh->stat.st_mtim.tv_nsec = 0;

    // 将更新好的数据写回DB
    UpdateMetadata(fh->stat, fh->ino);

    if(fh->fd != -1 ) {
        close(fh->fd);
        file_info->fh = -1;
        return 0;
    } else {
        if (ENABELD_LOG) {
            spdlog::warn("release: return failed");
        }
        return -ENOENT;
    }

}

int wrapperfs::Opendir(const char* path, struct fuse_file_info* file_info) {

    bool is_file;
    std::string filename;

    location_t *locate = new location_t();
    locate->tag = directory_relation;
 
    if(!PathLookup(path, locate->wrapper_id, is_file, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
        return -ENOENT;
    }

    if(!GetWrapperStat(locate->wrapper_id, &(locate->stat))) {
    
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot get the stat");
        }
        return -ENOENT;
    }

    file_info->fh = (uint64_t)locate;
    return 0;
}

// 
 int wrapperfs::Readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* file_info) {

    location_t* locate = (location_t *) file_info->fh;

    if (filler(buf, ".", NULL, 0) < 0) {
        if (ENABELD_LOG) {
            spdlog::warn("readdir error, single file");
        }      
        return -ENOENT;
    }

    if (filler(buf, "..", NULL, 0) < 0) {
        if (ENABELD_LOG) {
            spdlog::warn("readdir error, single file");
        }      
        return -ENOENT;
    }

    // 获取文件
    entries_t* entries = new entries_t;
    entries->tag = directory_relation;
    entries->wrapper_id = locate->wrapper_id;
    if (!get_entries(adaptor_, entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("readdir error, cannot find sub file entry!");
            }
            return -ENOENT;
    } else {
        for (auto &entry : entries->list) {
            if (filler(buf, entry.second.c_str(), NULL, 0) < 0) {
                if (ENABELD_LOG) {
                    spdlog::warn("readdir error, cannot filler filename");
                }
                delete entries;
                return -ENOENT;
            } else {
                continue;
            }
        }
        delete entries;
    }

    // 获取目录
    std::vector<relation_t> relations;
 
    if (!get_range_relations(adaptor_, directory_relation, locate->wrapper_id, relations)) {
        if (ENABELD_LOG) {
                spdlog::warn("readdir error, cannot find sub directory!");
            }
            return 0;
    } else {
        for (auto &relation : relations) {
            if (filler(buf, relation.distance.c_str(), NULL, 0) < 0) {
                if (ENABELD_LOG) {
                    spdlog::warn("readdir error, cannot filler directory name.");
                }
                return -ENOENT;
            } else {
                continue;
            }
        }
    }
    return 0;
}

// 删除目录之前，会不断调用unlink删除文件，因此只需要删除目录相关的东西就行
int wrapperfs::RemoveDir(const char *path) {

    std::string path_string = path;
    std::vector<std::string> path_items = split_string(path_string, PATH_DELIMITER);
    size_t wrapper_id_in_search = ROOT_WRAPPER_ID;

    // 解析根目录和包装器标签目录
    if (path_items.size() < 2) {

        // 为根目录
        if (path_items.size() == 0) {
            if (ENABELD_LOG) {
                spdlog::warn("rmdir: cannot rm root dir!");
            }
            return -ENOENT;
        } 
    } else {
        PathResolution(path_items, wrapper_id_in_search);
    }

    std::string filename = path_items[path_items.size() - 1];
    size_t ino;
    size_t wrapper_id;

    if (!WrapperLookup(wrapper_id_in_search, wrapper_id, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("rmdir: cannot resolved the path!");
        }
        return -ENOENT;
       
    }

    // 删除relation
    relation_t* relation = new relation_t;
    relation->tag = directory_relation;
    relation->wrapper_id = wrapper_id_in_search;
    relation->distance = filename;
    relation->next_wrapper_id = wrapper_id;

    if(!delete_relation(adaptor_, relation))  {
             
        if (ENABELD_LOG) {
            spdlog::warn("rmdir error: cannot delete relation!");
        }
        return -ENONET;
    }

    // 删除location
    location_t* locate = new location_t;
    locate->wrapper_id = wrapper_id;
    locate->tag = directory_relation;

    if(!delete_location(adaptor_, locate)) {
        if (ENABELD_LOG) {
            spdlog::warn("rmdir error: cannot delete location!");
        }
        return -ENONET;
    }

    // 删除entries
    entries_t* entries = new entries_t;
    entries->tag = directory_relation;
    entries->wrapper_id = locate->wrapper_id;

    if (!delete_entries(adaptor_, entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("rmdir error: cannot delete entries!");
            }
            return -ENOENT;
    }
    return 0;
}

//
int wrapperfs::Releasedir(const char* path, struct fuse_file_info* file_info) {

    location_t *locate = reinterpret_cast<location_t*> (file_info->fh);

    // 释放句柄
    if (locate != NULL) {
        delete locate;
        file_info->fh = -1;
        return 0;
    } else {
        if(ENABELD_LOG) {
            spdlog::warn("release: return failed");
        }
        return -ENOENT;
    }
}



int wrapperfs::Access(const char* path, int mask) {

    if (ENABELD_LOG) {
        spdlog::debug("access: %s %08x\n", path, mask);
    }

    return 0;
}

// bug: 文件的时间总是不对哈！(solved)
int wrapperfs::UpdateTimes(const char* path, const struct timespec tv[2]) {

  
    struct stat stat;
    std::string filename;
    size_t ino;

    size_t wrapper_id = ROOT_WRAPPER_ID;
    std::vector<std::string> path_items = split_string(path, PATH_DELIMITER);
    filename = path_items[path_items.size() - 1];

    if (path_items.size() > 1 ) {
        PathResolution(path_items, wrapper_id);
    }

    if(!EntriesLookup(wrapper_id, ino, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
        return -ENOENT;
    }

    if(!GetFileStat(wrapper_id, filename, ino, &stat)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot get the stat");
        }
        return -ENOENT;
    }

    // 更新时间
    stat.st_atim.tv_sec  = tv[0].tv_sec;
    stat.st_atim.tv_nsec = tv[0].tv_nsec;
    stat.st_mtim.tv_sec  = tv[1].tv_sec;
    stat.st_mtim.tv_nsec = tv[1].tv_nsec;

    // 将更新好的数据写回DB
    UpdateMetadata(stat, ino);

    if (ENABELD_LOG) {
        spdlog::debug("updateTimes");
    }
    return 0;
}

int wrapperfs::Chmod(const char *path, mode_t mode) {

    bool is_file;
    std::string filename;
    size_t wrapper_id;
    size_t ino;
    struct stat statbuf;

    if(!PathLookup(path, wrapper_id, is_file, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
         return -ENOENT;
    }

    if (is_file == true)   {
        if(!GetFileStat(wrapper_id, filename, ino, &statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get file stat error");
            }
            return -ENOENT;
        }
        statbuf.st_mode = mode;
        UpdateMetadata(statbuf, ino);

    } else {
        if(!GetWrapperStat(wrapper_id, &statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get wrapper stat error");
            }
            return -ENOENT; 
        } 
        statbuf.st_mode = mode;
        UpdateWrapperMetadata(statbuf, wrapper_id);
    }
    return 0;
}

int wrapperfs::Chown(const char *path, uid_t uid, gid_t gid) {

    bool is_file;
    std::string filename;
    size_t wrapper_id;
    size_t ino;
    struct stat statbuf;

    if(!PathLookup(path, wrapper_id, is_file, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
         return -ENOENT;
    }

    if (is_file == true)   {
        if(!GetFileStat(wrapper_id, filename, ino, &statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get file stat error");
            }
            return -ENOENT;
        }
        statbuf.st_gid = gid;
        statbuf.st_uid = uid;
        UpdateMetadata(statbuf, ino);

    } else {
        if(!GetWrapperStat(wrapper_id, &statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get wrapper stat error");
            }
            return -ENOENT; 
        } 
        statbuf.st_gid = gid;
        statbuf.st_uid = uid;
        UpdateWrapperMetadata(statbuf, wrapper_id);
    }
    return 0;

}


void wrapperfs::GetFilePath(size_t ino, std::string &path) {
    std::stringstream ss;
    ss << (ino >> NUM_FILES_IN_DATADIR_BITS);
    
    std::string dir_path = combine_path(data_dir_, ss.str());
    if(!std::filesystem::exists(dir_path.c_str())) {
        int ret = mkdir(dir_path.c_str(), 0777);
        if (ret != 0) {
            if (ENABELD_LOG) {
                spdlog::debug("cannot create directory into the data dir!");
            }
        }
    }

    ss  <<  "/" << (ino % NUM_FILES_IN_DATADIR);
    path = combine_path(data_dir_, ss.str());

}

}