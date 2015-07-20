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

#include "foedus/cxx11.hpp"

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
#include <atomic>  // NOLINT(build/include_order): for the ifdef
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

namespace foedus {
namespace assorted {

/// Defines the implementations in this header.
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS

template <typename T>
inline bool raw_atomic_compare_exchange_strong_inl(T* target, T* expected, T desired) {
#if defined(__GNUC__)
  T expected_val = *expected;
  T old_val = ::__sync_val_compare_and_swap(target, expected_val, desired);
  if (old_val == expected_val) {
    return true;
  } else {
    *expected = old_val;
    return false;
  }
#else  // defined(__GNUC__)
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  // this is super ugly. but this is the only way to do it without compiler-dependent code.
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  return casted->compare_exchange_strong(*expected, desired);
#endif  // defined(__GNUC__)
}

template <typename T>
inline T raw_atomic_exchange_inl(T* target, T desired) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  return casted->exchange(desired);
}

template <typename T>
inline T raw_atomic_fetch_add_inl(T* target, T addendum) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  return casted->fetch_add(addendum);
}

template <typename T>
inline T raw_atomic_load_seq_cst_inl(const T* target) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  const std::atomic<T>* casted = reinterpret_cast< const std::atomic<T>* >(target);
  return casted->load(std::memory_order_seq_cst);
}

template <typename T>
inline void raw_atomic_store_seq_cst_inl(T* target, T value) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  casted->store(value, std::memory_order_seq_cst);
}

template <typename T>
inline void raw_atomic_store_release_inl(T* target, T value) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  casted->store(value, std::memory_order_release);
}

#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

template <typename T>
bool  raw_atomic_compare_exchange_strong_cpp(T* target, T* expected, T desired);

template <typename T>
T     raw_atomic_exchange_cpp(T* target, T desired);

template <typename T>
T     raw_atomic_fetch_add_cpp(T* target, T addendum);

template <typename T>
T     raw_atomic_load_seq_cst_cpp(const T* target);

template <typename T>
void  raw_atomic_store_seq_cst_cpp(T* target, T value);

template <typename T>
void  raw_atomic_store_release_cpp(T* target, T value);

/**
 * @brief Strong atomic CAS for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @ingroup ASSORTED
 * @details
 * std::atomic provides atomic CAS, but it requires the data to be std::atomic<T>, rather than T
 * itself like what gcc's builtin function provides. This is problemetic when we have to store
 * T rather than std::atomic<T>. This method fills the gap.
 *
 * This header \e tries to inline this method. So, it defines the entire method
 * if DISABLE_CXX11_IN_PUBLIC_HEADERS is not given. Otherwise, it has to delegate to
 * the definition in cpp (_cpp function).
 * \note The implementation of this method currently does a \e quite ugly stuff.
 * Still, better to be ugly in one place rather than in many places...
 */
template <typename T>
inline bool raw_atomic_compare_exchange_strong(T* target, T* expected, T desired) {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_compare_exchange_strong_inl<T>(target, expected, desired);
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_compare_exchange_strong_cpp<T>(target, expected, desired);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
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
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_exchange_inl<T>(target, desired);
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_exchange_cpp<T>(target, desired);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Atomic fetch-add for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @return result of arithmetic addition of the value and addendum
 * @ingroup ASSORTED
 */
template <typename T>
inline T raw_atomic_fetch_add(T* target, T addendum) {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_fetch_add_inl<T>(target, addendum);
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_fetch_add_cpp<T>(target, addendum);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Atomic load with a seq_cst barrier for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @return result of load
 * @ingroup ASSORTED
 */
template <typename T>
inline T raw_atomic_load_seq_cst(const T* target) {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_load_seq_cst_inl<T>(target);
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
  return raw_atomic_load_seq_cst_cpp<T>(target);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Atomic store with a seq_cst barrier for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @ingroup ASSORTED
 */
template <typename T>
inline void raw_atomic_store_seq_cst(T* target, T value) {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  raw_atomic_store_seq_cst_inl<T>(target, value);
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
  raw_atomic_store_seq_cst_cpp<T>(target, value);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Atomic store with a release barrier for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @ingroup ASSORTED
 */
template <typename T>
inline void raw_atomic_store_release(T* target, T value) {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  raw_atomic_store_release_inl<T>(target, value);
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
  raw_atomic_store_release_cpp<T>(target, value);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_RAW_ATOMICS_HPP_
