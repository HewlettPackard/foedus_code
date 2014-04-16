/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/assorted_func.hpp>
#include <stdint.h>
#include <cstdlib>
namespace foedus {
namespace assorted {

int64_t int_div_ceil(int64_t dividee, int64_t dividor) {
    std::ldiv_t result = std::div(dividee, dividor);
    return result.rem != 0 ? (result.quot + 1) : result.quot;
}

#if defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
bool atomic_compare_exchange_strong_uint128(
    uint64_t *ptr, uint64_t o1, uint64_t o2, uint64_t n1, uint64_t n2) {
    __uint128_t old_value, new_value;
    reinterpret_cast<uint64_t*>(&old_value)[0] = o1;
    reinterpret_cast<uint64_t*>(&old_value)[1] = o2;
    reinterpret_cast<uint64_t*>(&new_value)[0] = n1;
    reinterpret_cast<uint64_t*>(&new_value)[1] = n2;
    return ::__sync_bool_compare_and_swap(
        reinterpret_cast<__uint128_t*>(ptr), old_value, new_value);
}
#else  // defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
bool atomic_compare_exchange_strong_uint128(
    uint64_t *ptr, uint64_t o1, uint64_t o2, uint64_t n1, uint64_t n2) {
    // oh well, then resort to assembly
    // see: linux/arch/x86/include/asm/cmpxchg_64.h
    bool result;
    uint64_t junk;
    asm volatile("lock; cmpxchg16b %2;setz %1"
        : "=d"(junk), "=a"(result), "+m" (*ptr)
        : "b"(n1), "c"(n2), "a"(o1), "d"(o2));
    return result;
}
#endif  // defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)


}  // namespace assorted
}  // namespace foedus

