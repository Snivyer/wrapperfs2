#include "fs/wrapperfs.h"

namespace wrapperfs {



inline static void BuildLocationKey(size_t wrapper_id, location_key &key,
                                    wrapper_tag tag = wrapper_tag::directory_relation) {
    key.tag = tag;
    key.wrapper_id = wrapper_id;
}

inline static void BuildRelationKey(size_t wrapper_id, std::string &dist, relation_key &key,
                                    wrapper_tag tag = wrapper_tag::directory_relation) {
    key.tag = tag;
    key.wrapper_id = wrapper_id;
    key.distance = dist;
}

inline static void BuildEntryKey(size_t wrapper_id, entry_key &key,
                                wrapper_tag tag = wrapper_tag::directory_relation)  {
    key.wrapper_id = wrapper_id;
    key.tag = tag;
}


struct stat* GetMetadata(rnode_header* &rh) {
    return reinterpret_cast<struct stat*> (rh);
}

struct stat* GetMetadata(location_header* &rh) {
    return reinterpret_cast<struct stat*> (rh);
}

// wrapperfs的初始化过程
// 初始化的关键参数有：max_ino, max_wrapper_id
wrapperfs::wrapperfs(const std::string &data_dir, const std::string &db_dir) {
    max_ino = 0;
    max_wrapper_id = ROOT_WRAPPER_ID;
    data_dir_ = data_dir;
    adaptor_ = new LevelDBAdaptor(db_dir);
    rnode_handle = new RnodeHandle(adaptor_);
    wrapper_handle = new WrapperHandle(adaptor_);
}


 bool wrapperfs::WrapperLookup(size_t &wrapper_id, size_t &next_wrapper_id, std::string &distance) {

    if(distance == "/") {
        next_wrapper_id = wrapper_id;
        return true;
    }

     // 首先判断是否能够找到目录
    relation_key key;
    BuildRelationKey(wrapper_id, distance, key);
    // 如果能够找到关系，那么就是目录
    if (wrapper_handle->get_relation(key.ToString(), next_wrapper_id) ) {
        return true;
    }
    return false;
 }

 bool wrapperfs::EntriesLookup(size_t &wrapper_id, size_t &ino, std::string &primary_attr, entry_value* &eval) {

    entry_key key;
    BuildEntryKey(wrapper_id, key);

    // 如果能够找到，那么就是文件
     if (wrapper_handle->get_entries(key.ToString(), eval)) {
        auto ret = eval->vmap->find(primary_attr);
        if(ret != eval->vmap->end()) {
            ino = ret->second;
            return true;
        }

        // if(eval->find(primary_attr, ino))  {
        //     return true;
        // }
    } 
    return false;
 }

bool wrapperfs::ParentPathLookup(const char *path,
                               size_t &wrapper_id,
                               size_t &wrapper_id_in_search,
                               const char* &lastdelimiter) {
  const char* lpos = path;
  const char* rpos;
  std::string item;
  wrapper_id_in_search = ROOT_WRAPPER_ID;
  while ((rpos = strchr(lpos+1, PATH_DELIMITER)) != NULL) {
    if (rpos - lpos > 0) {
        relation_key key;
        std::string filename = std::string(lpos+1, rpos-lpos-1);
        BuildRelationKey(wrapper_id_in_search, filename, key);
        if (!wrapper_handle->get_relation(key.ToString(), wrapper_id_in_search)) {
            return false;
        }
    }
    lpos = rpos;
  }

  if(lpos == path) {
    wrapper_id = ROOT_WRAPPER_ID;
  }

  lastdelimiter = lpos;
  return true;
}
 
bool wrapperfs::PathLookup(const char* path, size_t &wrapper_id, std::string &filename) {
    
    const char* lpos;
    size_t wrapper_in_search;
    if(ParentPathLookup(path, wrapper_id, wrapper_in_search, lpos)) {
        const char* rpos = strchr(lpos, '\0');
        if (rpos != NULL && rpos-lpos > 1) {
            // fixme: 获取最后一级的wrapper_id和文件名
            wrapper_id = wrapper_in_search;
            filename = std::string(lpos + 1, rpos-lpos-1);
        } else {
            filename = std::string(lpos, 1);
        }
        return true;
    } else {
        return false;
    }   
}

// 这是返回给
bool wrapperfs::GetFileStat(size_t &ino, struct stat* stat) {
   
    rnode_header* rh;
    if (!rnode_handle->get_rnode(ino, rh))
    {
        if (ENABELD_LOG) {
            spdlog::warn("cannot get file stat");
        }
            return false;
    } else {
        memcpy(stat, rh, sizeof(struct stat));
        return true;
    }
}

bool wrapperfs::GetWrapperStat(size_t wrapper_id, struct stat *stat) {

    location_key key;
    std::string lval;
    BuildLocationKey(wrapper_id, key);

    location_header* lh;

    if (!wrapper_handle->get_location(key.ToString(), lh)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr: return failed");
        }
        return false;
    } else {
        memcpy(stat, lh, sizeof(struct stat));
        return true;
    }  
}


 int wrapperfs::Getattr(const char* path, struct stat* statbuf) {

    size_t wrapper_id;
    size_t pc_id;
    std::string filename;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr error: no such file or directory");
        }
        return -ENOENT;
    }

    if(WrapperLookup(wrapper_id, pc_id, filename)) {
        op_s.getDirStat += 1;
        if(!GetWrapperStat(pc_id, statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get wrapper stat error");
            }
            return -ENOENT; 
        } 
        return 0;
    }

    entry_value* eval = nullptr;;
    if(EntriesLookup(wrapper_id, pc_id, filename, eval)) {
        if(!GetFileStat(pc_id, statbuf)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get file stat error");
            }
            return -ENOENT;
        }
        return 0;
    }
    return -ENOENT;
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


int wrapperfs::Mknod(const char* path, mode_t mode, dev_t dev) {

    size_t wrapper_id;
    std::string filename;
    PathLookup(path, wrapper_id, filename);

    op_s.mknod += 1;
    max_ino = max_ino + 1;
    size_t ino = max_ino;

    // FIXME: mode需要加入S_IFREG，以标注是文件
    rnode_header* header = new rnode_header;
    InitStat(header->fstat, ino, mode | S_IFREG, dev);
    rnode_handle->write_rnode(ino, header, metadata_status::create);


    // 还需要将文件名作为额外元数据写进去
    entry_key key;
    BuildEntryKey(wrapper_id, key);
    entry_value* eval = nullptr;
    if (!wrapper_handle->get_entries(key.ToString(), eval)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: cannot get name metadata.");
        }
        return -ENONET;
    } 
    eval->vmap->insert({filename, ino});
    //eval->push(filename, ino);
    wrapper_handle->change_entries_stat(key.ToString());
    return 0;
}

// FIXME: 这里在创建目录stat的时候，采用的是wrapper_id作为inode id传进去的，不知道有没有问题 （没问题）
int wrapperfs::Mkdir(const char* path, mode_t mode) {

    size_t wrapper_id;
    std::string filename;

    PathLookup(path, wrapper_id, filename);
    
    max_wrapper_id = max_wrapper_id + 1;
    size_t create_wrapper_id = max_wrapper_id;

    location_header* lheader = new location_header;
    // FIXME: 需要加上S_IFDIR，以标记是目录
    InitStat(lheader->fstat, create_wrapper_id, mode | S_IFDIR, 0);

    // 首先创建location，将wrapper写进去
    location_key lkey;
    BuildLocationKey(create_wrapper_id, lkey);
    wrapper_handle->write_location(lkey.ToString(), lheader, metadata_status::create);
    

    // 还需要将目录关系写进去
    relation_key rkey;
    BuildRelationKey(wrapper_id, filename, rkey);
    wrapper_handle->write_relation(rkey.ToString(), create_wrapper_id, metadata_status::create);
             
    // 最后将空entries写进去
    entry_key ekey;
    BuildEntryKey(create_wrapper_id, ekey);

    entry_value* eval = new entry_value;
    wrapper_handle->write_entries(ekey.ToString(), eval, metadata_status::create);
    return 0;
}

//
int wrapperfs::Open(const char* path, struct fuse_file_info* file_info) {

    size_t wrapper_id;
    size_t ino;
    std::string filename;
    file_handle_t* fh = new file_handle_t;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr error: no such file or directory");
        }
        return -ENOENT;
    }

    op_s.open += 1;
    entry_value* eval = nullptr;;
    if(!EntriesLookup(wrapper_id, ino, filename, eval)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
        return -ENOENT;
    }

    rnode_header* header;
    if(!rnode_handle->get_rnode(ino, header)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot get the stat");
        }
        return -ENOENT;
    }

    fh->stat = GetMetadata(header);
    fh->ino = ino;
    fh->flags = file_info->flags;
    std::string real_path;
    GetFilePath(ino, real_path);

    fh->fd = open(real_path.c_str(), file_info->flags | O_CREAT, fh->stat->st_mode);
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
        fh->fd = open(path_string.c_str(), fh->flags | O_CREAT, fh->stat->st_mode);
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
        fh->fd = open(path_string.c_str(), fh->flags | O_CREAT, fh->stat->st_blksize);
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
        fh->stat->st_size = offset + size;
          
        // 将更新好的数据写回DB
        rnode_handle->change_stat(fh->ino);
    }
    return ret;
}

int wrapperfs::Unlink(const char *path) {
    
    bool is_remove = false;
    size_t wrapper_id;
    size_t ino;
    std::string filename;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("Mkdir error: no such parent file or directory");
        }
        return -ENOENT;
    }
   
    op_s.unlink += 1;

    entry_value* eval = nullptr;
    if(!EntriesLookup(wrapper_id, ino, filename, eval)) {
        if (ENABELD_LOG) {
            spdlog::warn("unlink error, cannot get entries");
        }
        return -ENOENT;
    } else {
        entry_key ekey;
        BuildEntryKey(wrapper_id, ekey);

        eval->vmap->erase(filename);
       // eval->remove(filename);
        wrapper_handle->change_entries_stat(ekey.ToString());

        if (rnode_handle->change_stat(ino, metadata_status::remove)) {
            //std::async(std::launch::async, &RnodeHandle::sync, this->rnode_handle, ino);
        }

        is_remove = true;
    }

    if (is_remove) {
        // 删除真实的文件
        std::string real_path = path;
        GetFilePath(ino, real_path);
        return unlink(real_path.c_str());
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

    fh->stat->st_atim.tv_sec  = time(NULL);
    fh->stat->st_atim.tv_nsec = 0;
    fh->stat->st_mtim.tv_sec  = time(NULL);
    fh->stat->st_mtim.tv_nsec = 0;

    // 将更新好的数据写回DB
    rnode_handle->change_stat(fh->ino);
  
    if(fh->fd != -1) {
        close(fh->fd);
        file_info->fh = -1;
        delete fh;
        return 0;
    } else {
        if (ENABELD_LOG) {
            spdlog::warn("release: return failed");
        }
        return -ENOENT;
    }
}

int wrapperfs::Opendir(const char* path, struct fuse_file_info* file_info) {

    size_t wrapper_id;
    std::string filename;
    struct wrapper_handle_t* wh = new wrapper_handle_t;
    wh->tag =wrapper_tag::directory_relation;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("Mkdir error: no such parent file or directory");
        }
        return -ENOENT;
    }

    op_s.opendir += 1;
    if (!WrapperLookup(wrapper_id, wh->wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("opendir: cannot resolved the path!");
        }
        return -ENOENT;
    } 
    
    struct location_header* header;
    struct location_key key;
    BuildLocationKey(wh->wrapper_id, key);

    if(!wrapper_handle->get_location(key.ToString(), header)) {
        if (ENABELD_LOG) {
            spdlog::warn("opendir: cannot get the stat");
        }
        return -ENOENT;
    }

    wh->stat = GetMetadata(header);
    file_info->fh = (uint64_t)wh;
    return 0;
}


// 
 int wrapperfs::Readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* file_info) {

    struct wrapper_handle_t* wh = (wrapper_handle_t*) file_info->fh;
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
    struct entry_key key;
    BuildEntryKey(wh->wrapper_id, key);
    struct entry_value* eval = nullptr;

    if (!wrapper_handle->get_entries(key.ToString(), eval)) {
            if (ENABELD_LOG) {
                spdlog::warn("readdir error, cannot find sub file entry!");
            }
            return -ENOENT;
    } else {
        for (auto item : *(eval->vmap)) {
            if (filler(buf, item.first.c_str(), NULL, 0) < 0) {
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
    struct relation_key rkey;
    std::string nullstr = std::string("");
    BuildRelationKey(wh->wrapper_id, nullstr, rkey);

    ATTR_STR_LIST list;
    if(wrapper_handle->get_relations(rkey, list)) {
        for (auto &item : list) {
            std::vector<std::string> items = split_string(item.first, ":");
            if (filler(buf, items[items.size() - 1].c_str(), NULL, 0) < 0) {
                if (ENABELD_LOG) {
                    spdlog::warn("readdir error, cannot filler directory name.");
                }
                return -ENOENT;
            } 
        }
    }
    return 0;
}

// 删除目录之前，会不断调用unlink删除文件，因此只需要删除目录相关的东西就行
int wrapperfs::RemoveDir(const char *path) {

    size_t wrapper_id;
    size_t pc_id;
    std::string filename;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr error: no such file or directory");
        }
        return -ENOENT;
    }

    op_s.rmdir += 1;
    if (!WrapperLookup(wrapper_id, pc_id, filename)) {

        if (ENABELD_LOG) {
            spdlog::warn("rmdir: cannot resolved the path!");
        }
        return -ENOENT;
       
    }

    // 删除relation
    struct relation_key rkey;
    BuildRelationKey(wrapper_id, filename, rkey);
    wrapper_handle->change_relation_stat(rkey.ToString(), metadata_status::remove);
        //std::async(std::launch::async, &WrapperHandle::sync_relation, this->wrapper_handle, rkey.ToString());
        //wrapper_handle->sync_relation(rkey);


    struct location_key lkey;
    BuildLocationKey(pc_id, lkey);
    if(wrapper_handle->change_stat(lkey.ToString(), metadata_status::remove)) {
        //std::async(std::launch::async, &WrapperHandle::sync_location, this->wrapper_handle, lkey.ToString());
    }

  
    // 删除entries
    struct entry_key key;
    BuildEntryKey(pc_id, key);
    if(wrapper_handle->change_entries_stat(key.ToString(), metadata_status::remove)) {
        //std::async(std::launch::async, &WrapperHandle::sync_entries, this->wrapper_handle, key.ToString());
    }
    return 0;
}

//
int wrapperfs::Releasedir(const char* path, struct fuse_file_info* file_info) {

    struct wrapper_handle_t* wh = (wrapper_handle_t*) file_info->fh;
    op_s.releasedir += 1;

    // 释放句柄
    if (wh != NULL) {
        file_info->fh = -1;
        location_key lkey;
        entry_key ekey;
        BuildEntryKey(wh->wrapper_id, ekey);
        BuildLocationKey(wh->wrapper_id, lkey);
        delete wh;
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

    struct rnode_header *header;
    size_t wrapper_id;
    size_t ino;
    std::string filename;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr error: no such file or directory");
        }
        return -ENOENT;
    }

    op_s.utimens += 1;
    entry_value* eval = nullptr;;
    if(!EntriesLookup(wrapper_id, ino, filename, eval)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot resolved the path!");
        }
        return -ENOENT;
    }

    if(!rnode_handle->get_rnode(ino, header)) {
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot get the stat");
        }
        return -ENOENT;
    }

    // 更新时间
    header->fstat.st_atim.tv_sec  = tv[0].tv_sec;
    header->fstat.st_atim.tv_nsec = tv[0].tv_nsec;
    header->fstat.st_mtim.tv_sec  = tv[1].tv_sec;
    header->fstat.st_mtim.tv_nsec = tv[1].tv_nsec;
    rnode_handle->change_stat(ino);

    if (ENABELD_LOG) {
        spdlog::debug("updateTimes");
    }
    return 0;
}

int wrapperfs::Chmod(const char *path, mode_t mode) {

    size_t wrapper_id;
    size_t pc_id;
    std::string filename;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr error: no such file or directory");
        }
        return -ENOENT;
    }

    entry_value* eval = nullptr;;
    if(EntriesLookup(wrapper_id, pc_id, filename, eval)) {
        struct rnode_header *header;
        if(!rnode_handle->get_rnode(pc_id, header)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get file stat error");
            }
            return -ENOENT;
        }
        header->fstat.st_mode = mode;
        rnode_handle->change_stat(pc_id);
        return 0;
    }

    if(WrapperLookup(wrapper_id, pc_id, filename)) {
        struct location_key key;
        BuildLocationKey(pc_id, key);
        struct location_header* header;
        if(wrapper_handle->get_location(key.ToString(), header)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get wrapper stat error");
            }
            return -ENOENT; 
        } 
        header->fstat.st_mode = mode;
        wrapper_handle->change_stat(key.ToString());
        return 0;
    }
    return -ENOENT;
}

int wrapperfs::Chown(const char *path, uid_t uid, gid_t gid) {

    size_t wrapper_id;
    size_t pc_id;
    std::string filename;

    if(!PathLookup(path, wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr error: no such file or directory");
        }
        return -ENOENT;
    }

    op_s.chown += 1;
    entry_value* eval = nullptr;;
    if(EntriesLookup(wrapper_id, pc_id, filename, eval)) {
        struct rnode_header *header;
        if(!rnode_handle->get_rnode(pc_id, header)) {
                if (ENABELD_LOG) {
                spdlog::warn("chown: get file stat error");
                }
        } else {
            header->fstat.st_gid = gid;
            header->fstat.st_uid = uid;
            rnode_handle->change_stat(pc_id);
            return 0;
        }
    }

    if(WrapperLookup(wrapper_id, pc_id, filename)) {
        struct location_key key;
        BuildLocationKey(pc_id, key);
        struct location_header* header;
        if(wrapper_handle->get_location(key.ToString(), header)) {
            if (ENABELD_LOG) {
                spdlog::warn("getattr: get wrapper stat error");
            }
            return -ENOENT; 
        } 
        header->fstat.st_gid = gid;
        header->fstat.st_uid = uid;
        wrapper_handle->change_stat(key.ToString());
        return 0;
    }

    return -ENOENT;
}

int wrapperfs::Rename(const char* source, const char* dest) {

    // 还是需要分文件还是目录
    std::string source_filename, dest_filename;
    size_t source_wrapper_id, dest_wrapper_id;
    size_t source_ino;
    struct entry_value *eval;

    op_s.rename += 1;
    if(!PathLookup(source, source_wrapper_id, source_filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr error: no such file or directory");
        }
        return -ENOENT;
    }
    PathLookup(dest, dest_wrapper_id, dest_filename);
        
    if(EntriesLookup(source_wrapper_id, source_ino, source_filename, eval)) {
        
        // step1: 删除source的entries
        entry_key ekey;
        BuildEntryKey(source_wrapper_id, ekey);
        eval->vmap->erase(source_filename);
        //eval->remove(source_filename);
        wrapper_handle->change_relation_stat(ekey.ToString());
 
        // step2: 添加dest的entries
        struct entry_key dest_key;
        BuildEntryKey(dest_wrapper_id, dest_key);
        entry_value *dest_eval = nullptr;
                               
        if (!wrapper_handle->get_entries(dest_key.ToString(), dest_eval)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error: dir isn't exist, cannot mv file into this dir!");
            }
            return -ENONET;
        } 

        dest_eval->vmap->insert({dest_filename, source_ino});
        //dest_eval->push(dest_filename, source_ino);
        wrapper_handle->change_entries_stat(dest_key.ToString());

        return 0;

    } else {

        // step1: 删除source的relation
        struct relation_key key;
        size_t pc_id;
        BuildRelationKey(source_wrapper_id, source_filename, key);
        wrapper_handle->get_relation(key.ToString(), pc_id);
        wrapper_handle->change_relation_stat(key.ToString(), metadata_status::remove);
            //std::async(std::launch::async, &WrapperHandle::sync_relation, this->wrapper_handle, key.ToString());
            //wrapper_handle->sync_relation(key.ToString());
        
   
        // step2: 添加dest的relation
        BuildRelationKey(dest_wrapper_id, dest_filename, key);
        wrapper_handle->write_relation(key.ToString(), pc_id);
        return 0;
    }
 }

void wrapperfs::Destroy(void *data) {

    if (STATISTICS_LOG) {
        spdlog::info("IO statics {}", io_s.debug());
        spdlog::info("operation statics {}", op_s.debug());
    }

    std::future<bool> ret = std::async(std::launch::async, &RnodeHandle::syncs, this->rnode_handle);
    wrapper_handle->sync();

    if(wrapper_handle) {
        delete wrapper_handle;
    }
    
    if(rnode_handle && ret.get() == true) {
        delete rnode_handle;
    }
        
    if(adaptor_) {
        delete adaptor_;
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