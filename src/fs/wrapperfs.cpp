#include "fs/wrapperfs.h"

namespace wrapperfs {

// wrapperfs的初始化过程
// 初始化的关键参数有：max_ino, max_wrapper_id
wrapperfs::wrapperfs(const std::string &data_dir, const std::string &db_dir) {
    max_ino = 0;
    max_wrapper_id = ROOT_WRAPPER_ID;
    data_dir_ = data_dir;
    adaptor_ = new LevelDBAdaptor(db_dir);
    inode_handle = new InodeHandle(adaptor_);
    wrapper_handle = new WrapperHandle(adaptor_);
}

bool wrapperfs::PathResolution(std::vector<std::string> &path_items, size_t &wrapper_id_in_search) {

    int index = -1;
    while (index != path_items.size() - 2) {
        
        relation_t* relation = new relation_t;
        relation->tag = directory_relation;
        relation->wrapper_id = wrapper_id_in_search;
        relation->distance = path_items[index + 1];

        if (!wrapper_handle->get_relation(relation)) {
            break;
        } else {
                index += 1;
                wrapper_id_in_search  = relation->next_wrapper_id;
        }
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
    if (wrapper_handle->get_relation(relation) ) {
        next_wrapper_id = relation->next_wrapper_id;
        return true;
    }
    return false;
 }

 bool wrapperfs::EntriesLookup(size_t &wrapper_id, size_t &ino, std::string &primary_attr) {

    entries_t* entries = new entries_t; 
    entries->tag = directory_relation;
    entries->wrapper_id = wrapper_id;

    // 如果能够找到，那么就是文件
     if (wrapper_handle->get_entries(entries)) {
        for (auto &entry : entries->list) {
            if (primary_attr == entry.second) {
                ino = entry.first;
                return true;
            }
        }
    } 
    return false;
 }

 
bool wrapperfs::PathLookup(const char* path, size_t &wrapper_id, bool &is_file, std::string &filename) {

    size_t pc_id;
    PathLookup(path,wrapper_id, is_file, filename, pc_id);
}

// bug: 无法区分目录或文件是新目录，还是旧目录 (solved: 新目录或新文件将返回false)
// pc_id: parent id or child id
bool wrapperfs::PathLookup(const char* path, size_t &wrapper_id, bool &is_file, std::string &filename, size_t &pc_id) {

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
        pc_id = wrapper_id_in_search;
        return true;
    }

    if (EntriesLookup(wrapper_id_in_search, ino, filename)) {
        is_file = true;
        wrapper_id = wrapper_id_in_search;
        pc_id = ino;
        return true;
    }

    wrapper_id = wrapper_id_in_search;
    pc_id = wrapper_id_in_search;
    return false;

}

// bug: 在get_entries过程中出现了段错误，原因出自于delete metadata，在这个过程中metadata的地址发生了几次变化，导致delete不掉了 (solved)
// bug: 在get_entries过程中出现了段错误 (solved 查找成功可以delete，查找失败则不能)
// FIXME: 这里的逻辑可以省略，直接拿metadata就行
bool wrapperfs::GetFileStat(size_t &ino, struct stat *stat) {

    
    inode_metadata_t* metadata = new inode_metadata_t;

        if (!inode_handle->get_inode_metadata(ino, metadata)) {
            if (ENABELD_LOG) {
                spdlog::warn("getStat error, single file");
            }
                return false;
        } else {
            std::memcpy(stat, &metadata->stat, sizeof(struct stat));
            return true;
        }
}

bool wrapperfs::GetWrapperStat(size_t wrapper_id, struct stat *stat) {
    
    location_t* location = new location_t;
    location->tag = directory_relation;
    location->wrapper_id = wrapper_id;
    if (!wrapper_handle->get_location(location)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr: return failed");
        }
        return false;
    } else {
        std::memcpy(stat, &location->stat, sizeof(struct stat));
        return true;
    }  
}


bool wrapperfs::UpdateMetadata(struct stat &stat, size_t ino) {
    
    inode_metadata_t *metadata = new inode_metadata_t;
    std::memcpy(&(metadata->stat), &stat, sizeof(struct stat));
        
    if (!inode_handle->put_inode_metadata(ino, metadata)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: put file stat error");
        }
        return -ENONET;
    }

}

bool wrapperfs::UpdateWrapperMetadata(struct stat &stat, size_t wrapper_id) {

    // 首先创建location，将wrapper写进去
    location_t* locate = new location_t;
    locate->wrapper_id = wrapper_id;
    locate->tag = directory_relation;
    std::memcpy(&(locate->stat), &stat, sizeof(struct stat));

    if(!wrapper_handle->put_location(locate)) {
        if (ENABELD_LOG) {
            spdlog::warn("mkdir error: cannot create directory wrapper.");
        }
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

    op_s.getattr += 1;

    if(!PathLookup(path, wrapper_id, is_file, filename, ino)) {

        op_s.getNULLStat += 1;
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
         return -ENOENT;
    }

    if (is_file == true)   {
        op_s.getFileStat += 1;
        if(!GetFileStat(ino, statbuf)) {

            if (ENABELD_LOG) {
                spdlog::warn("getattr: get file stat error");
            }
            return -ENOENT;
        }  
    } else {
        op_s.getDirStat += 1;
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

    op_s.mknod += 1;

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
    

    // bug: 这里的缓存会失效！（solved）
    if (!wrapper_handle->get_entries(entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: cannot get name metadata.");
        }
        return -ENONET;
    } 
        
    entries->list.push_back(std::pair(ino, filename)); 
    if(!wrapper_handle->put_entries(entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: cannot put name metadata into db.");
        }
        return -ENONET;
    }

    return 0;
}

// FIXME: 这里在创建目录stat的时候，采用的是wrapper_id作为inode id传进去的，不知道有没有问题
int wrapperfs::Mkdir(const char* path, mode_t mode) {

    size_t wrapper_id = ROOT_WRAPPER_ID;
    std::vector<std::string> path_items = split_string(path, PATH_DELIMITER);
    std::string filename = path_items[path_items.size() - 1];

    op_s.mkdir += 1;

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

    if(!wrapper_handle->put_location(locate)) {
        if (ENABELD_LOG) {
            spdlog::warn("mkdir error: cannot create directory wrapper.");
        }
        return -ENONET;
    }

    // 还需要将目录关系写进去
    relation_t* relation = new relation_t;
    relation->tag = directory_relation;
    relation->wrapper_id = wrapper_id;
    relation->distance = filename;
    relation->next_wrapper_id = create_wrapper_id;

    if(!wrapper_handle->put_relation(relation))  {
             
        if (ENABELD_LOG) {
            spdlog::warn("mkdir error: cannot create the relation of dirctory wrapper.");
        }
        return -ENONET;
    }

    // 最后将空entries写进去
    entries_t* entries = new entries_t;
    entries->wrapper_id = create_wrapper_id;
    entries->tag = directory_relation;

    if(!wrapper_handle->put_entries(entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("mkfs error: cannot create empty entries.");
        }
        return -ENONET;
    }
    return 0;
}

//
int wrapperfs::Open(const char* path, struct fuse_file_info* file_info) {

    std::string filename;
    size_t ino;
    file_handle_t* fh = new file_handle_t;

    op_s.open += 1;

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

    if(!GetFileStat(ino, &(fh->stat))) {

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

    op_s.read += 1;

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
   op_s.write += 1;

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

    op_s.unlink += 1;

    std::vector<std::string> path_items = split_string(path, PATH_DELIMITER);
    filename = path_items[path_items.size() - 1];

    if (path_items.size() > 1 ) {
        PathResolution(path_items, wrapper_id);
    }

    entries_t* entries = new entries_t;
    entries->tag = directory_relation;
    entries->wrapper_id = wrapper_id;

    if (!wrapper_handle->get_entries(entries)) {
        if (ENABELD_LOG) {
            spdlog::warn("unlink error, cannot get entries");
        }
        return -ENOENT;
    } else {
        auto it = entries->list.begin();
        while (it != entries->list.end()) {
            if (filename == (*it).second) {
                ino = (*it).first;
                // 删除元数据
                if(!inode_handle->delete_inode_metadata((*it).first)) {
                    if (ENABELD_LOG) {
                        spdlog::warn("delete file error");
                    }
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
        if (!wrapper_handle->put_entries(entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("delete file error, can not put deleted entries");
            }
            return -ENOENT;
        }

        // 删除真实的文件
        std::string real_path = path;
        GetFilePath(ino, real_path);
        unlink(real_path.c_str());


        return 0;
    } else {

        if (ENABELD_LOG) {
            spdlog::warn("delete file error, can not find the deleted file.");
        }
        return -ENOENT;
    }
}

//
int wrapperfs::Release(const char* path, struct fuse_file_info* file_info) {

    file_handle_t* fh = reinterpret_cast<file_handle_t*> (file_info->fh);
    op_s.release += 1;

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
    op_s.opendir += 1;

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
    op_s.readdir += 1;

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
    if (!wrapper_handle->get_entries(entries)) {
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
                return -ENOENT;
            } else {
                continue;
            }
        }
    }

    // 获取目录
    std::vector<relation_t> relations;
 
    if (!wrapper_handle->get_range_relations(directory_relation, locate->wrapper_id, relations)) {
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

    op_s.rmdir += 1;

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

    if(!wrapper_handle->delete_relation(relation))  {
             
        if (ENABELD_LOG) {
            spdlog::warn("rmdir error: cannot delete relation!");
        }
        return -ENONET;
    }

    // 删除location
    location_t* locate = new location_t;
    locate->wrapper_id = wrapper_id;
    locate->tag = directory_relation;

    if(!wrapper_handle->delete_location(locate)) {
        if (ENABELD_LOG) {
            spdlog::warn("rmdir error: cannot delete location!");
        }
        return -ENONET;
    }

    // 删除entries
    entries_t* entries = new entries_t;
    entries->tag = directory_relation;
    entries->wrapper_id = locate->wrapper_id;

    if (!wrapper_handle->delete_entries(entries)) {
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
    op_s.releasedir += 1;

    // 释放句柄
    if (locate != NULL) {
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

    op_s.access += 1;

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
    op_s.utimens += 1;

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

    if(!GetFileStat(ino, &stat)) {
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

    op_s.chmod += 1;

    if(!PathLookup(path, wrapper_id, is_file, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
         return -ENOENT;
    }

    if (is_file == true)   {
        if(!GetFileStat(ino, &statbuf)) {
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

    op_s.chown += 1;

    if(!PathLookup(path, wrapper_id, is_file, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
         return -ENOENT;
    }

    if (is_file == true)   {
        if(!GetFileStat(ino, &statbuf)) {
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


int wrapperfs::Rename(const char* source, const char* dest) {

    // 还是需要分文件还是目录
    bool is_file;
    std::string source_filename, dest_filename;
    size_t source_wrapper_id, dest_wrapper_id;
    size_t source_ino;
    size_t source_pc_id, dest_pc_id;
    op_s.rename += 1;

    if(!PathLookup(source, source_wrapper_id, is_file, source_filename, source_pc_id)) {

        if (ENABELD_LOG) {
            spdlog::warn("rename: cannot resolved the source path!");
        }
         return -ENOENT;
    }

    PathLookup(dest, dest_wrapper_id, is_file, dest_filename, dest_pc_id);

    if(is_file) {

        // step1: 删除source的entries
        entries_t* source_entries = new entries_t;
        source_entries->tag = directory_relation;
        source_entries->wrapper_id = source_wrapper_id;
        
        if (!wrapper_handle->get_entries(source_entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error, cannot get source file entries");
            }
            return -ENOENT;
        } else {
            auto it = source_entries->list.begin();
            while (it != source_entries->list.end()) {
                if (source_filename == (*it).second) {
                    // 复制ino id
                    source_ino = (*it).first;
                    // 移除entry
                    source_entries->list.erase(it);
                    break;
                } else {
                    it++;
                }
            }
        }

        // step2: 将修改后的source_entries存回去
        if(!wrapper_handle->put_entries(source_entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error: cannot put modified entries!");
            }
            return -ENONET;
        }

        // step3: 添加dest的entries
        entries_t* dest_entries = new entries_t;
        dest_entries->wrapper_id = dest_wrapper_id;     // mv可以到不同的目录下
        dest_entries->tag = directory_relation;
        
        if (!wrapper_handle->get_entries(dest_entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error: dir isn't exist, cannot mv file into this dir!");
            }
            return -ENONET;
        } 
        
        dest_entries->list.push_back(std::pair(source_ino, dest_filename)); 
        if(!wrapper_handle->put_entries(dest_entries)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error: cannot put dest entries.");
            }
        }
        return 0;
    } else {

        // step1: 删除source的relation
        relation_t* source_relation = new relation_t;
        source_relation->tag = directory_relation;
        source_relation->wrapper_id = source_pc_id;
        source_relation->distance = source_filename;
        source_relation->next_wrapper_id = source_wrapper_id;

        if(!wrapper_handle->delete_relation(source_relation))  {
             
            if (ENABELD_LOG) {
                spdlog::warn("rename error: cannot delete source relation!");
            }
            return -ENONET;
        }

        // step2: 添加dest的relation
        relation_t* dest_relation = new relation_t;
        dest_relation->tag = directory_relation;
        dest_relation->wrapper_id = dest_pc_id;
        dest_relation->distance = dest_filename;
        dest_relation->next_wrapper_id = source_wrapper_id;

        if(!wrapper_handle->put_relation(dest_relation))  {
             
            if (ENABELD_LOG) {
                spdlog::warn("rename error: cannot put dest relation!");
            }
            return -ENONET;
        }
        return 0;
    }

 }

 void wrapperfs::Destroy(void *data) {

    if (STATISTICS_LOG) {
        spdlog::info("IO statics {}", io_s.debug());
        spdlog::info("operation statics {}", op_s.debug());
    }
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