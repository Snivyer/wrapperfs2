#include "fs/wrapperfs.h"

namespace wrapperfs {






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


bool wrapperfs::PathResolution(std::vector<std::string> &path_items, size_t &wrapper_id_in_search) {

    int index = -1;
    while (index != path_items.size() - 2) {
        relation_key key;
        BuildRelationKey(wrapper_id_in_search, directory_relation, path_items[index + 1], key);
        
        if (!wrapper_handle->get_relation(key, wrapper_id_in_search)) {
            break;
        } else {
                index += 1;
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
    relation_key key;
    BuildRelationKey(wrapper_id, directory_relation, distance, key);
 
    if (wrapper_handle->read_relation(key, next_wrapper_id) ) {
        return true;
    }
    return false;
 }

 bool wrapperfs::EntriesLookup(size_t &wrapper_id, size_t &ino, std::string &primary_attr) {

    entry_key key;
    BuildEntryKey(wrapper_id, directory_relation, key);
    std::string result;

    // 如果能够找到，那么就是文件
    if (wrapper_handle->get_entry(ino, primary_attr)) {
        return true;
    }

    if(wrapper_handle->read_entries(key, result)) {
        entry_value eval(result);
        if(eval.find(primary_attr, ino))  {
            return true;
        }
    } 
    return false;
 }

 
bool wrapperfs::PathLookup(const char* path, size_t &wrapper_id, bool &is_file, std::string &filename) {

    size_t pc_id;
    return PathLookup(path,wrapper_id, is_file, filename, pc_id);
}

// bug: 无法区分目录或文件是新目录，还是旧目录 (solved: 新目录或新文件将返回false)
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

    std::future<bool> ret = std::async(std::launch::async, &wrapperfs::EntriesLookup, this,
         std::ref(wrapper_id_in_search), std::ref(ino), std::ref(filename));

    
    if (WrapperLookup(wrapper_id_in_search, wrapper_id, filename)) {
        is_file = false;
        pc_id = wrapper_id_in_search;
        return true;
    }

    if(ret.get() == true) {
        is_file = true;
        wrapper_id = wrapper_id_in_search;
        pc_id = ino;
        return true;

    }

    wrapper_id = wrapper_id_in_search;
    pc_id = wrapper_id_in_search;
    return false;

}


bool wrapperfs::GetFileStat(size_t &ino, struct stat *stat) {

    stat = rnode_handle->get_rnode(ino);
    
    if (stat == nullptr) {
        rnode_key key;
        BuildRnodeKey(ino, key);
        std::string value;
        if (!rnode_handle->get_rnode(key, value)) 
        {    
            if (ENABELD_LOG) {
                spdlog::warn("cannot get file stat");
            }
            return false; 
        } else {
            *stat = *(GetMetadata(value));
        }
    }
    return true;
}

bool wrapperfs::GetWrapperStat(size_t wrapper_id, struct stat *stat) {

    location_key key;
    std::string lval;
    BuildLocationKey(wrapper_id, directory_relation, key);

    if (!wrapper_handle->read_location(key, lval)) {
        if (ENABELD_LOG) {
            spdlog::warn("getattr: return failed");
        }
        return false;
    } else {
        *stat = *(GetMetadata(lval));
        return true;
    }  
}


// bug: 一进入这个函数就会报错，初步还以是rnode_value &rval的问题
bool wrapperfs::UpdateMetadata1(mode_t mode, dev_t dev, size_t ino) {

    
    rnode_key key;
    BuildRnodeKey(ino, key);
    rnode_header* header = new rnode_header;
    InitStat(header->fstat, ino, mode, dev);
    std::string header_value = std::string(reinterpret_cast<const char*>(header), sizeof(rnode_header));
  
    if (!rnode_handle->put_rnode(key, header_value)) {
        if (ENABELD_LOG) {
            spdlog::warn("update metadata error.");
        }
        return false;
    }
    return true;
}


bool wrapperfs::UpdateMetadata2(struct stat &stat, size_t ino) {

    rnode_key key;
    BuildRnodeKey(ino, key);

    rnode_header* header = new rnode_header;
    std::memcpy(&(header->fstat), &stat, sizeof(struct stat));
    std::string header_value = std::string(reinterpret_cast<const char*>(header), sizeof(rnode_header));
  
    if (!rnode_handle->put_rnode(key, header_value)) {
        if (ENABELD_LOG) {
            spdlog::warn("update metadata error.");
        }
        return false;
    }
    return true;
}

bool wrapperfs::UpdateWrapperMetadata(struct stat &stat, size_t wrapper_id) {

    location_key key;
    BuildLocationKey(wrapper_id, directory_relation, key);
    location_header* lheader = new location_header;
    std::memcpy(&(lheader->fstat), &stat, sizeof(struct stat));
    std::string lval = std::string(reinterpret_cast<const char*>(lheader), sizeof(location_header));

    if(!wrapper_handle->put_location(key, lval)) {
        if (ENABELD_LOG) {
            spdlog::warn("mkdir error: cannot create directory wrapper.");
        }
        return -ENONET;
    }
}


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
// bug: 经过测试，代码的问题就是在这些指针的转换过程中，不转换就不会错
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

    UpdateMetadata1(mode | S_IFREG, dev, ino);

    if(!wrapper_handle->put_entry(wrapper_id, file_handle, ino)) {
        if (ENABELD_LOG) {
            spdlog::warn("mknod error: cannot put name metadata into db.");
        }
        return -ENONET;
    }
    return 0;
}

bool wrapperfs::putLocation(wrapper_tag tag, size_t wrapper_id, struct stat &stat) {

    location_key lkey;
    BuildLocationKey(wrapper_id, directory_relation, lkey);

    location_header* lheader = new location_header;
    std::memcpy(&(lheader->fstat), &stat, sizeof(struct stat));
    std::string lval = std::string(reinterpret_cast<const char*>(lheader), sizeof(location_header));

    if(!wrapper_handle->put_location(lkey, lval)) {
        return false;
    }

    return true;
}

bool wrapperfs::putRelation(wrapper_tag tag, size_t wrapper_id, std::string filename, size_t next_wrapper_id) {

    relation_key rkey;
    BuildRelationKey(wrapper_id, directory_relation, filename, rkey);
    if(!wrapper_handle->put_relation(rkey, next_wrapper_id))  {
        return false;
    }
    return true;
}

bool wrapperfs::deleteLocation(wrapper_tag tag, size_t wrapper_id) {
    location_key lkey;
    BuildLocationKey(wrapper_id, directory_relation, lkey);
    if(!wrapper_handle->delete_location(lkey)) {
        return false;
    }
    return true;
}


bool wrapperfs::deleteRelation(wrapper_tag tag, size_t wrapper_id, std::string filename) {
    
    relation_key rkey;
    BuildRelationKey(wrapper_id, directory_relation, filename, rkey);
    if(!wrapper_handle->delete_relation(rkey))  {
        return false;
    }
    return true;

}

// FIXME: 这里在创建目录stat的时候，采用的是wrapper_id作为inode id传进去的，不知道有没有问题 （没问题）
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
    std::future<bool> lret = std::async(std::launch::async, &wrapperfs::putLocation, this,
        directory_relation, create_wrapper_id, std::ref(stat));
    
    // 还需要将目录关系写进去
    
    std::future<bool> rret = std::async(std::launch::async, &wrapperfs::putRelation, this,
        directory_relation, wrapper_id, filename, create_wrapper_id);

    // 最后将空entries写进去
    entry_key ekey;
    BuildEntryKey(create_wrapper_id, directory_relation, ekey);

    entry_value eval;
    std::string result = eval.ToString();
    if(!wrapper_handle->put_entries(ekey, result)) {
        
        if(lret.get() == true) {
            deleteLocation(directory_relation, wrapper_id);
        }

        if(rret.get() == true) {
            deleteRelation(directory_relation, wrapper_id, filename);
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
        UpdateMetadata2(fh->stat, fh->ino);
    }
    return ret;
}

int wrapperfs::Unlink(const char *path) {

    std::string filename;
    size_t ino;
    size_t wrapper_id = ROOT_WRAPPER_ID;

    op_s.unlink += 1;

    std::vector<std::string> path_items = split_string(path, PATH_DELIMITER);
    filename = path_items[path_items.size() - 1];

    if (path_items.size() > 1 ) {
        PathResolution(path_items, wrapper_id);
    }

    ino = wrapper_handle->get_entry(wrapper_id, filename);

    while (ino == 0) {
        entry_key key;
        BuildEntryKey(wrapper_id, directory_relation, key);

        std::string result;
        if (!wrapper_handle->read_entries(key, result)) {
            return -ENOENT;
        }

        ino = wrapper_handle->get_entry(wrapper_id, filename);
    }

    if(!wrapper_handle->remove_entry(wrapper_id, filename)) {
        if (ENABELD_LOG) {
            spdlog::warn("unlink error, cannot get entries");
        }
        return -ENOENT;
    }

    rnode_key key;
    BuildRnodeKey(ino, key);

    if(!rnode_handle->delete_rnode(key)) {
            if (ENABELD_LOG) {
                spdlog::warn("delete file error");
            }
            return -ENOENT;
    }

   

    entry_value eval(result);
    if(eval.find(filename, ino)) {
      
        // 删除元数据
        
        eval.remove(filename);
        is_remove = true;
    }
    
    if (is_remove) {
        result = eval.ToString();
        if (!wrapper_handle->put_entries(key, result)) {
            if (ENABELD_LOG) {
                spdlog::warn("delete file error, can not put deleted entries");
            }
            return -ENOENT;
        }

        //  
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
    UpdateMetadata2(fh->stat, fh->ino);

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

    op_s.opendir += 1;
    wrapper_handle_t* wh = new wrapper_handle_t;
    wh->tag = directory_relation;


    std::string path_string = path;
    std::vector<std::string> path_items = split_string(path_string, PATH_DELIMITER);
    size_t wrapper_id = ROOT_WRAPPER_ID;
    std::string filename;


    if (path_items.size() == 0) {
        wh->wrapper_id = ROOT_WRAPPER_ID;
    } else {
        if (path_items.size() > 1) {
            PathResolution(path_items, wrapper_id);
        }

        filename = path_items[path_items.size() - 1];
        if (!WrapperLookup(wrapper_id, wh->wrapper_id, filename)) {
            if (ENABELD_LOG) {
                spdlog::warn("opendir: cannot resolved the path!");
            }
            return -ENOENT;
        } 
    }
    
    if(!GetWrapperStat(wh->wrapper_id, &(wh->stat))) {
    
        if (ENABELD_LOG) {
            spdlog::warn("open: cannot get the stat");
        }
        return -ENOENT;
    }

    file_info->fh = (uint64_t)wh;
    return 0;
}

 bool wrapperfs::rangeGetEntries(wrapper_tag tag, size_t wrapper_id, ATTR_LIST &wid2attr) {

    relation_key rkey;
    std::string nullstr = std::string("");
    BuildRelationKey(wrapper_id, directory_relation, nullstr, rkey);

    if (!wrapper_handle->get_range_relations(rkey, wid2attr)) {
        return false;
    }

    return true;
 }

// 
 int wrapperfs::Readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* file_info) {

    wrapper_handle_t* wh = (wrapper_handle_t*) file_info->fh;
    
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

    ATTR_LIST wid2attr;

    // 你可以慢慢地去范围查找哈，我们不急的
    std::future<bool> ret = std::async(std::launch::async, &wrapperfs::rangeGetEntries, this,
         directory_relation, wh->wrapper_id, std::ref(wid2attr));

    // 获取文件
    entry_key key;
    BuildEntryKey(wh->wrapper_id, directory_relation, key);
    std::string result;

    if (!wrapper_handle->get_entries(key, result)) {
            if (ENABELD_LOG) {
                spdlog::warn("readdir error, cannot find sub file entry!");
            }
            return -ENOENT;
    } else {

        entry_value eval(result);
        std::vector<std::string> list;
        eval.ToList(list);
        
        for (auto item : list) {
            if (filler(buf, item.c_str(), NULL, 0) < 0) {
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
    if(ret.get() == false) {
        return 0;
    } else {
        for (auto &item : wid2attr) {
            if (filler(buf, item.first.c_str(), NULL, 0) < 0) {
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
    std::future<bool> rret = std::async(std::launch::async, &wrapperfs::deleteRelation, this,
         directory_relation, wrapper_id_in_search, filename);

    // 删除location
    std::future<bool> lret = std::async(std::launch::async, &wrapperfs::deleteLocation, this,
         directory_relation, wrapper_id);
   

    // 删除entries
    entry_key key;
    BuildEntryKey(wrapper_id, directory_relation, key);
    if (!wrapper_handle->delete_entries(key)) {
            if (ENABELD_LOG) {
                spdlog::warn("rmdir error: cannot delete entries!");
            }
            // FIXME: 如果误删除了，就不恢复了噢
            return -ENOENT;
    }
    return 0;
}

//
int wrapperfs::Releasedir(const char* path, struct fuse_file_info* file_info) {

    wrapper_handle_t* wh = (wrapper_handle_t*) file_info->fh;

    op_s.releasedir += 1;

    // 释放句柄
    if (wh != NULL) {
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
    UpdateMetadata2(stat, ino);

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
        UpdateMetadata2(statbuf, ino);

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
        UpdateMetadata2(statbuf, ino);

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
        entry_key source_key;
        BuildEntryKey(source_wrapper_id, directory_relation, source_key);
        std::string result;
        

        if (!wrapper_handle->get_entries(source_key, result)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error, cannot get source file entries");
            }
            return -ENOENT;
        } 

        entry_value eval(result);
        if(eval.find(source_filename, source_ino)) {
            eval.remove(source_filename);
        }

        result = eval.ToString();
            
        // step2: 将修改后的source_entries存回去
        if(!wrapper_handle->put_entries(source_key, result)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error: cannot put modified entries!");
            }
            return -ENONET;
        }

        // step3: 添加dest的entries
        entry_key dest_key;
        BuildEntryKey(dest_wrapper_id, directory_relation, dest_key);
        std::string dest_result;
 
        if (!wrapper_handle->get_entries(dest_key, dest_result)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error: dir isn't exist, cannot mv file into this dir!");
            }
            return -ENONET;
        } 

        entry_value  dest_eval(dest_result);
        dest_eval.push(dest_filename, source_ino);
        dest_result = dest_eval.ToString();
        
        if(!wrapper_handle->put_entries(dest_key, dest_result)) {
            if (ENABELD_LOG) {
                spdlog::warn("rename error: cannot put dest entries.");
            }
        }
        return 0;
    } else {

        // step1: 删除source的relation
        relation_key key;
        BuildRelationKey(source_pc_id, directory_relation, source_filename, key);

        if(!wrapper_handle->delete_relation(key))  {
             
            if (ENABELD_LOG) {
                spdlog::warn("rename error: cannot delete source relation!");
            }
            return -ENONET;
        }

        // step2: 添加dest的relation
        BuildRelationKey(dest_pc_id, directory_relation, dest_filename, key);
        if(!wrapper_handle->put_relation(key, source_wrapper_id))  {
             
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