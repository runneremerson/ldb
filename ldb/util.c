#include "util.h"

#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <stdio.h>



int compare_with_length(const void* s1, size_t l1, const void* s2, size_t l2){
    const size_t min_l = (l1 < l2) ? l1 : l2;
    int r = memcmp(s1, s2, min_l);
    if(r == 0){
        if(l1 < l2) r = -1;
        else if(l1 > l2) r = +1;
    }
    return r;
}

uint64_t time_ms(){
    struct timeval now;
    gettimeofday(&now, NULL);
    return now.tv_sec*1000 + now.tv_usec/1000;
}

void printbuf(const char *buf, size_t length){
    uint8_t c;
    size_t i=0;
    for(; i<length; i++){
        c = buf[i];
        if(c == 0){
            printf("00 ");
            
        }else{
            printf("%2x ", c );
        }
    }
    printf("\n");
}

uint16_t big_endian_u16(uint16_t v){
	return (v>>8) | (v<<8);
}

uint32_t big_endian_u32(uint32_t v){
	return (v >> 24) | ((v >> 8) & 0xff00) | ((v << 8) & 0xff0000) | (v << 24);
}

uint64_t big_endian_u64(uint64_t v){
	uint32_t h = v >> 32;
	uint32_t l = v & 0xffffffffull;
	return ((uint64_t)big_endian_u32(h)) | ((uint64_t)big_endian_u32(l) << 32);
}

