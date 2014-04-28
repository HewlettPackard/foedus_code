/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/assorted/raw_atomics.hpp>
#include <stdint.h>
#include <atomic>
namespace foedus {
namespace assorted {

template <typename T>
bool    raw_atomic_compare_exchange_strong(T* target, T* expected, T desired) {
    // this is super ugly. but this is the only way to do it without compiler-dependent code.
    std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
    return std::atomic_compare_exchange_strong<T>(casted, expected, desired);
}
// template explicit instantiations for all integer types.
#define EXPLICIT_INSTANTIATION_STRONG(x) \
    template bool raw_atomic_compare_exchange_strong(x *target, x *expected, x desired)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_STRONG);

#if defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
bool raw_atomic_compare_exchange_strong_uint128(
    uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value) {
    return ::__sync_bool_compare_and_swap(
        reinterpret_cast<__uint128_t*>(ptr),
        reinterpret_cast<const __uint128_t*>(old_value),
        reinterpret_cast<const __uint128_t*>(new_value));
}
#else  // defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
bool raw_atomic_compare_exchange_strong_uint128(
    uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value) {
    // oh well, then resort to assembly
    // see: linux/arch/x86/include/asm/cmpxchg_64.h
    bool result;
    uint64_t junk;
    asm volatile("lock; cmpxchg16b %2;setz %1"
        : "=d"(junk), "=a"(result), "+m" (*ptr)
        : "b"(new_value[0]), "c"(new_value[1]), "a"(old_value[0]), "d"(old_value[1]));
    return result;
}
#endif  // defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)


}  // namespace assorted
}  // namespace foedus

