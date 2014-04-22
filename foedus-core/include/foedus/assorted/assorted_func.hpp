/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
#define FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
#include <foedus/cxx11.hpp>
#include <stdint.h>
#include <string>
#include <typeinfo>
namespace foedus {
namespace assorted {

/**
 * @brief Returns the smallest multiply of ALIGNMENT that is equal or larger than the given number.
 * @ingroup ASSORTED
 * @tparam T integer type
 * @tparam ALIGNMENT alignment size. must be power of two
 * @details
 * In other words, round-up. For example of 8-alignment, 7 becomes 8, 8 becomes 8, 9 becomes 16.
 * @see https://en.wikipedia.org/wiki/Data_structure_alignment
 * @see Hacker's Delight 2nd Ed. Chap 3-1.
 */
template <typename T, unsigned int ALIGNMENT>
inline T align(T value) {
    return static_cast<T>((value + ALIGNMENT - 1) & (-ALIGNMENT));
}

/**
 * 8-alignment.
 * @ingroup ASSORTED
 */
template <typename T> inline T align8(T value) { return align<T, 8>(value); }

/**
 * 64-alignment.
 * @ingroup ASSORTED
 */
template <typename T> inline T align64(T value) { return align<T, 64>(value); }


/**
 * Efficient ceil(dividee/dividor) for integer.
 * @ingroup ASSORTED
 */
int64_t int_div_ceil(int64_t dividee, int64_t dividor);

/**
 * Alternative for static_assert(sizeof(foo) == sizeof(bar), "oh crap") to display sizeof(foo).
 * @ingroup ASSORTED
 * @details
 * Use it like this:
 * @code{.cpp}
 * const int dummy_check1_ = assorted::static_size_check<sizeof(foo), sizeof(bar)>();
 * @endcode
 * Do not forget "const" because otherwise you'll get multiple-definition errors in hpp.
 */
template<uint64_t SIZE1, uint64_t SIZE2>
inline int static_size_check() {
    CXX11_STATIC_ASSERT(SIZE1 == SIZE2,
        "Static Size Check failed. Look for a message like this to see the value of Size1 and "
        "Size2: 'In instantiation of int foedus::assorted::static_size_check() [with long unsigned"
        " int SIZE1 = <size1>ul; long unsigned int SIZE2 = <size2>ul]'");
    return 0;
}

/**
 * @brief Atomic 128-bit CAS, which is not in the standard yet.
 * @param[in,out] ptr Points to 128-bit data. \b MUST \b BE \b 128-bit \b ALIGNED.
 * @param[in] old_value Points to 128-bit data. If ptr holds this value, we swap.
 * @param[in] new_value Points to 128-bit data. We change the ptr to hold this value.
 * @return Whether the swap happened
 * @ingroup ASSORTED
 * @details
 * We shouldn't rely on it too much as double-word CAS is not provided in older CPU.
 * Once the C++ standard employs it, this method should go away. I will be graybeard by then, tho.
 * \attention You need to give "-mcx16" to GCC to use its builtin 128bit CAS.
 * Otherwise, __GCC_HAVE_SYNC_COMPARE_AND_SWAP_16 is not set and we have to resort to x86 assembly.
 * Check out "gcc -dM -E - < /dev/null".
 */
bool atomic_compare_exchange_strong_uint128(
    uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value);

/**
 * @brief Weak version of atomic_compare_exchange_strong_uint128().
 * @ingroup ASSORTED
 */
inline bool atomic_compare_exchange_weak_uint128(
    uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value) {
    if (ptr[0] != old_value[0] || ptr[1] != old_value[1]) {
        return false;  // this comparison is fast but not atomic, thus 'weak'
    } else {
        return atomic_compare_exchange_strong_uint128(ptr, old_value, new_value);
    }
}

std::string demangle_type_name(const char* mangled_name);

template <typename T>
std::string get_pretty_type_name() {
    return demangle_type_name(typeid(T).name());
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
