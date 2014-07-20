/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_RAW_ATOMICS_HPP_
#define FOEDUS_ASSORTED_RAW_ATOMICS_HPP_
#include <stdint.h>
namespace foedus {
namespace assorted {
/**
 * @brief Strong atomic CAS for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @ingroup ASSORTED
 * @details
 * std::atomic provides atomic CAS, but it requires the data to be std::atomic<T>, rather than T
 * itself like what gcc's builtin function provides. This is problemetic when we have to store
 * T rather than std::atomic<T>. This method fills the gap.
 *
 * This header only declares this method. The definition and explicit instantiation are in the cpp.
 * \note The implementation of this method currently does a \e quite ugly stuff.
 * Still, better to be ugly in one place rather than in many places...
 */
template <typename T>
bool    raw_atomic_compare_exchange_strong(T* target, T* expected, T desired);


/**
 * @brief Weak version of raw_atomic_compare_exchange_strong().
 * @ingroup ASSORTED
 * @copydetails raw_atomic_compare_exchange_strong()
 */
template <typename T>
inline bool raw_atomic_compare_exchange_weak(T* target, T* expected, T desired) {
  if (*target != *expected) {
    *expected = *target;
    return false;
  } else {
    return raw_atomic_compare_exchange_strong<T>(target, expected, desired);
  }
}

/**
 * @brief Atomic 128-bit CAS, which is not in the standard yet.
 * @param[in,out] ptr Points to 128-bit data. \b MUST \b BE \b 128-bit \b ALIGNED.
 * @param[in] old_value Points to 128-bit data. If ptr holds this value, we swap.
 * Unlike std::atomic_compare_exchange_strong, this arg is const.
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
bool raw_atomic_compare_exchange_strong_uint128(
  uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value);

/**
 * @brief Weak version of raw_atomic_compare_exchange_strong_uint128().
 * @ingroup ASSORTED
 */
inline bool raw_atomic_compare_exchange_weak_uint128(
  uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value) {
  if (ptr[0] != old_value[0] || ptr[1] != old_value[1]) {
    return false;  // this comparison is fast but not atomic, thus 'weak'
  } else {
    return raw_atomic_compare_exchange_strong_uint128(ptr, old_value, new_value);
  }
}

/**
 * @brief Atomic fetch-add for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @return result of arithmetic addition of the value and addendum
 * @ingroup ASSORTED
 */
template <typename T>
T raw_atomic_fetch_add(T* target, T addendum);

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_RAW_ATOMICS_HPP_
