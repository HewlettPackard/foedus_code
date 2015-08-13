/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_ASSORTED_RAW_ATOMICS_HPP_
#define FOEDUS_ASSORTED_RAW_ATOMICS_HPP_

#include <stdint.h>

/**
 * @file foedus/assorted/raw_atomics.hpp
 * @ingroup ASSORTED
 * @brief Raw atomic operations that work for both C++11 and non-C++11 code.
 * @details
 * std::atomic provides atomic CAS etc, but it requires the data to be std::atomic<T>, rather than T
 * itself like what gcc's builtin function provides. This is problemetic when we have to store
 * T rather than std::atomic<T>. Also, we want to avoid C++11 in public headers.
 *
 * The methods in this header fill the gap.
 * This header defines, not just declares [1], the methods so that they are inlined.
 * We simply use gcc/clang's builtin. clang's gcc compatibility is so good that we don't need ifdef.
 *
 * [1] Except raw_atomic_compare_exchange_strong_uint128(). It's defined in cpp.
 *
 * @see foedus/assorted/atomic_fences.hpp
 */
namespace foedus {
namespace assorted {

/**
 * @brief Atomic CAS.
 * @tparam T integer type
 * @ingroup ASSORTED
 */
template <typename T>
inline bool raw_atomic_compare_exchange_strong(T* target, T* expected, T desired) {
  // Use newer builtin instead of __sync_val_compare_and_swap
  return ::__atomic_compare_exchange_n(
    target,
    expected,
    desired,
    false,
    __ATOMIC_SEQ_CST,
    __ATOMIC_SEQ_CST);
  // T expected_val = *expected;
  // T old_val = ::__sync_val_compare_and_swap(target, expected_val, desired);
  // if (old_val == expected_val) {
  //   return true;
  // } else {
  //   *expected = old_val;
  //   return false;
  // }
}

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
  uint64_t *ptr,
  const uint64_t *old_value,
  const uint64_t *new_value);

/**
 * @brief Weak version of raw_atomic_compare_exchange_strong_uint128().
 * @ingroup ASSORTED
 */
inline bool raw_atomic_compare_exchange_weak_uint128(
  uint64_t *ptr,
  const uint64_t *old_value,
  const uint64_t *new_value) {
  if (ptr[0] != old_value[0] || ptr[1] != old_value[1]) {
    return false;  // this comparison is fast but not atomic, thus 'weak'
  } else {
    return raw_atomic_compare_exchange_strong_uint128(ptr, old_value, new_value);
  }
}

/**
 * @brief Atomic Swap for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @param[in,out] target Points to the data to be swapped.
 * @param[in] desired This value will be installed.
 * @return returns the old value.
 * @ingroup ASSORTED
 * @details
 * This is a non-conditional swap, which always succeeds.
 */
template <typename T>
inline T raw_atomic_exchange(T* target, T desired) {
  return ::__atomic_exchange_n(target, desired, __ATOMIC_SEQ_CST);
  // Note: We must NOT use __sync_lock_test_and_set, which is only acquire-barrier for some
  // reason. We instead use GCC/Clang's __atomic_exchange() builtin.
  // return ::__sync_lock_test_and_set(target, desired);
  // see https://gcc.gnu.org/onlinedocs/gcc-4.4.3/gcc/Atomic-Builtins.html
  // and https://bugzilla.mozilla.org/show_bug.cgi?id=873799

  // BTW, __atomic_exchange_n/__ATOMIC_SEQ_CST demands a C++11-capable version of gcc/clang,
  // but FOEDUS anyway relies on C++11. It just allows the linked program to be
  // compiled without std=c++11. So, nothing lost.
}

/**
 * @brief Atomic fetch-add for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @return result of arithmetic addition of the value and addendum
 * @ingroup ASSORTED
 */
template <typename T>
inline T raw_atomic_fetch_add(T* target, T addendum) {
  return ::__atomic_fetch_add(target, addendum, __ATOMIC_SEQ_CST);
  // Just to align with above, use __atomic_fetch_add rather than __sync_fetch_and_add.
  // It's equivalent.
  // return ::__sync_fetch_and_add(target, addendum);
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_RAW_ATOMICS_HPP_
