/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/assorted_func.hpp>
#ifdef __GNUC__  // for get_pretty_type_name()
#include <cxxabi.h>
#endif  // __GNUC__
#include <stdint.h>
#include <cstdlib>
#include <string>
namespace foedus {
namespace assorted {

int64_t int_div_ceil(int64_t dividee, int64_t dividor) {
    std::ldiv_t result = std::div(dividee, dividor);
    return result.rem != 0 ? (result.quot + 1) : result.quot;
}

#if defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
bool atomic_compare_exchange_strong_uint128(
    uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value) {
    return ::__sync_bool_compare_and_swap(
        reinterpret_cast<__uint128_t*>(ptr),
        reinterpret_cast<const __uint128_t*>(old_value),
        reinterpret_cast<const __uint128_t*>(new_value));
}
#else  // defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
bool atomic_compare_exchange_strong_uint128(
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


std::string demangle_type_name(const char* mangled_name) {
#ifdef __GNUC__
    int status;
    char* demangled = abi::__cxa_demangle(mangled_name, nullptr, nullptr, &status);
    if (demangled) {
        std::string ret(demangled);
        ::free(demangled);
        return ret;
    }
#endif  // __GNUC__
    return mangled_name;
}



}  // namespace assorted
}  // namespace foedus

