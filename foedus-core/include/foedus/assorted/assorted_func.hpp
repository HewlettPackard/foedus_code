/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
#define FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
#include <foedus/cxx11.hpp>
#include <stdint.h>
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
 * @def foedus::assorted::atomic_compare_exchange_strong_uint128()
 * @brief Atomic 128-bit CAS, which is not in the standard yet.
 * @param[in,out] ptr Points to 128-bit data. \b MUST \b BE \b 128-bit \b ALIGNED.
 * @param[in] o1 If ptr holds this value, we swap. High 64 bit.
 * @param[in] o2 If ptr holds this value, we swap. Low 64 bit.
 * @param[in] n1 We change the ptr to hold this value. High 64 bit.
 * @param[in] n2 We change the ptr to hold this value. Low 64 bit.
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
    uint64_t *ptr, uint64_t o1, uint64_t o2, uint64_t n1, uint64_t n2);

/**
 * @def foedus::assorted::atomic_compare_exchange_weak_uint128()
 * @brief Weak version of atomic_compare_exchange_strong_uint128().
 * @ingroup ASSORTED
 */
inline bool atomic_compare_exchange_weak_uint128(
    uint64_t *ptr, uint64_t o1, uint64_t o2, uint64_t n1, uint64_t n2) {
    if (ptr[0] != o1 || ptr[1] != o2) {
        return false;  // this comparison is fast but not atomic, thus 'weak'
    } else {
        return atomic_compare_exchange_strong_uint128(ptr, o1, o2, n1, n2);
    }
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
