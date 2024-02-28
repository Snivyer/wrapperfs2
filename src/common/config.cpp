#include "common/config.h"

namespace wrapperfs {

bool ENABELD_LOG = true;
spdlog::level::level_enum DEFAULT_LOG_LEVEL = spdlog::level::info;
size_t ROOT_WRAPPER_ID = 0;
char* PATH_DELIMITER = "/";
size_t NUM_FILES_IN_DATADIR = 16384;
size_t NUM_FILES_IN_DATADIR_BITS = 14;

}