#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <string>
#include <iostream>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include "fs/wrapperfs.h"
#include "common/config.h"

static wrapperfs::wrapperfs* fs;

static int wrap_getattr(const char* path, struct stat* stat_buf) {

    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("getattr called: path - {}", path);
    }
    return fs->Getattr(path, stat_buf);
}

static int wrap_mknod(const char* path, mode_t mode, dev_t dev) {
    if (wrapperfs::ENABELD_LOG) {               
        spdlog::info("mknod called: path - {}", path);
    }
    return fs->Mknod(path, mode, dev);
}

static int wrap_mkdir(const char* path, mode_t mode) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("mkdir called: path - {}", path);
    }
    return fs->Mkdir(path, mode);
}

static int wrap_open(const char* path, struct fuse_file_info* file_info) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("open called: path - {}", path);
    }
    return fs->Open(path, file_info);
}

static int wrap_read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* file_info) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("read called: path - {}", path);
    }
    return fs->Read(path, buf, size, offset, file_info);
}

static int wrap_write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* file_info) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("write called: path - {}", path);
    }
    return fs->Write(path, buf, size, offset, file_info);
}

static int wrap_release(const char* path, struct fuse_file_info* file_info) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("release called: path - {}", path);
    }
    return fs->Release(path, file_info);
}

static int wrap_opendir(const char* path, struct fuse_file_info* file_info) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("opendir called: path - {}", path);
    }

     return fs->Opendir(path, file_info);
}

static int wrap_readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* file_info) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("readdir called: path - {}", path);
    }
    return fs->Readdir(path, buf, filler, offset, file_info);
}

static int wrap_releasedir(const char* path, struct fuse_file_info* file_info) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("releasedir called: path - {}", path);
    }
   return fs->Releasedir(path, file_info);
} 

static int wrap_access(const char* path, int mask) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("access called: path - {}", path);
    }
    return fs->Access(path, mask);
}

static int wrap_updatetime(const char *path, const struct timespec tv[2]) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("updateTime called: path - {}", path);
    }
    return fs->UpdateTimes(path, tv);
}

static int wrap_rmdir(const char *path)  {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("rmdir called: path - {}", path);
    }
    return fs->RemoveDir(path);
}

static int wrap_unlink(const char *path) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("unlink called: path - {}", path);
    }
    return fs->Unlink(path);

}

static int wrap_chmod(const char *path, mode_t mode)  {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("chmod called: path - {}", path);
    }
    return fs->Chmod(path, mode);

}

static int wrap_chown(const char *path, uid_t uid, gid_t gid) {
    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("chown called: path - {}", path);
    }
    return fs->Chown(path, uid, gid);
}

static struct fuse_operations operations = {
    .getattr = wrap_getattr,
    .mknod = wrap_mknod,
    .mkdir = wrap_mkdir,
    .unlink = wrap_unlink,
    .rmdir = wrap_rmdir,
    .chmod = wrap_chmod,
    .chown = wrap_chown,
    .open = wrap_open,
    .read = wrap_read,
    .write = wrap_write,
    .release = wrap_release,
    .opendir = wrap_opendir,
    .readdir = wrap_readdir,
    .releasedir = wrap_releasedir,
    .access = wrap_access,
    .utimens = wrap_updatetime,

};

int main(int argc, char *argv[]) {
    if (argc != 5) {
        std::cout << "wrapperfs mount error." << std::endl;
        return 1;
    }

    auto file_logger = spdlog::basic_logger_mt("file_logger", argv[4]);
    spdlog::set_default_logger(file_logger);
    spdlog::set_level(wrapperfs::DEFAULT_LOG_LEVEL);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    spdlog::flush_on(spdlog::level::trace);
  

    // show basic info
    std::cout << "mount_dir:" << argv[1] << std::endl;
    std::cout << "data_dir:" << argv[2] << std::endl;
    std::cout << "meta_dir:" << argv[3] << std::endl;
    std::cout << "log_dir:" << argv[4] << std::endl;

    if (wrapperfs::ENABELD_LOG) {
        spdlog::info("mount_dir: {}", argv[1]);
        spdlog::info("data_dir: {}", argv[2]);
        spdlog::info("meta_dir: {}", argv[3]);
        spdlog::info("log_dir: {}", argv[4]);
    }

    fs = new wrapperfs::wrapperfs(argv[2], argv[3]);
    int fuse_argc = 4;
    char* fuse_argv[] = { argv[0], argv[1], "-s", "-f"};
    std::cout << "start to run wrapperfs at " << argv[0] << " " << argv[1] << "." << std::endl;

    if (wrapperfs::ENABELD_LOG) {
        file_logger->info("wrapperfs have started!");
    }
    

    int fuse_stat = fuse_main(fuse_argc, fuse_argv, &operations, nullptr);
    if (fuse_stat != 0) {
        std::cout << "wrapperfs mount error." << std::endl;
    }
    return fuse_stat;
}