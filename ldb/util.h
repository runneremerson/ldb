#ifndef LDB_UTIL_H
#define LDB_UTIL_H

#include <stdint.h>
#include <stddef.h>


int compare_with_length(const void* s1, size_t l1, const void* s2, size_t l2);


uint64_t time_ms();

void printbuf(const char* buf, size_t length);


uint16_t big_endian_u16(uint16_t v);

uint32_t big_endian_u32(uint32_t v);

uint64_t big_endian_u64(uint64_t v);



#endif //LDB_UTIL_H

