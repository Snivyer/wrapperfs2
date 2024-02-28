#pragma once

#include <spdlog/spdlog.h>

namespace wrapperfs {

extern bool ENABELD_LOG;
extern spdlog::level::level_enum DEFAULT_LOG_LEVEL;
extern size_t ROOT_WRAPPER_ID;
extern char* PATH_DELIMITER;
extern size_t NUM_FILES_IN_DATADIR;
extern size_t NUM_FILES_IN_DATADIR_BITS;

}