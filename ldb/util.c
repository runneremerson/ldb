#include "util.h"

#include <string.h>



int compare_with_length(const void* s1, size_t l1, const void* s2, size_t l2){
    const size_t min_l = (l1 < l2) ? l1 : l2;
    int r = memcmp(s1, s2, min_l);
    if(r == 0){
        if(l1 < l2) r = -1;
        else if(l1 > l2) r = +1;
    }
    return r;
}
