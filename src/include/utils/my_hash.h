#ifndef HASH_H_
#define HASH_H_
#include <stdint.h>

namespace wrapperfs {

extern uint32_t crc32(const void * key, int len);

extern uint64_t murmur64(const void * key, int len, uint64_t seed);

}

#endif /* HASH_H_ */
