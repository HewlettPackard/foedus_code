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
#include "foedus/assorted/raw_atomics.hpp"

#include <stdint.h>

#include <atomic>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace assorted {

template <typename T>
bool    raw_atomic_compare_exchange_strong_cpp(T* target, T* expected, T desired) {
  return raw_atomic_compare_exchange_strong_inl<T>(target, expected, desired);
}

// template explicit instantiations for all integer types.
#define EXPLICIT_INSTANTIATION_STRONG(x) \
  template bool raw_atomic_compare_exchange_strong_cpp(x *target, x *expected, x desired)
INSTANTIATE_ALL_INTEGER_PLUS_BOOL_TYPES(EXPLICIT_INSTANTIATION_STRONG);

bool raw_atomic_compare_exchange_strong_uint128(
  uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value) {
  bool ret;
#if defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
  // gcc-x86 (-mcx16), then simply use __sync_bool_compare_and_swap.
  __uint128_t* ptr_casted = reinterpret_cast<__uint128_t*>(ptr);
  __uint128_t old_casted = *reinterpret_cast<const __uint128_t*>(old_value);
  __uint128_t new_casted = *reinterpret_cast<const __uint128_t*>(new_value);
  ret = ::__sync_bool_compare_and_swap(ptr_casted, old_casted, new_casted);
#elif defined(__GNUC__) && defined(__aarch64__)
  // gcc-AArch64 doesn't allow -mcx16. But, it supports __atomic_compare_exchange_16 with
  // libatomic.so. We need to link to it in that case.
  __uint128_t* ptr_casted = reinterpret_cast<__uint128_t*>(ptr);
  __uint128_t old_casted = *reinterpret_cast<const __uint128_t*>(old_value);
  __uint128_t new_casted = *reinterpret_cast<const __uint128_t*>(new_value);
  ret = ::__atomic_compare_exchange_16(
    ptr_casted,
    &old_casted,
    new_casted,
    false,              // strong CAS
    __ATOMIC_ACQ_REL,   // to make it atomic, of course acq_rel
    __ATOMIC_ACQUIRE);  // matters only when it fails. acquire is enough.
#else  // everything else
  // oh well, then resort to assembly, assuming x86. clang on ARMv8? oh please...
  // see: linux/arch/x86/include/asm/cmpxchg_64.h
  uint64_t junk;
  asm volatile("lock; cmpxchg16b %2;setz %1"
    : "=d"(junk), "=a"(ret), "+m" (*ptr)
    : "b"(new_value[0]), "c"(new_value[1]), "a"(old_value[0]), "d"(old_value[1]));
  // Note on roll-our-own non-gcc ARMv8 cas16. It's doable, but...
  // ARMv8 does have 128bit atomic instructions, called "pair" operations, such as ldaxp and stxp.
  // There is actually a library that uses it:
  // https://github.com/ivmai/libatomic_ops/blob/master/src/atomic_ops/sysdeps/gcc/aarch64.h
  // (but this is GPL. Don't open the URL unless you are ready for it.)
  // As of now (May 2014), GCC can't handle them, nor provide __uint128_t in ARMv8.
  // I think it's coming, however. I'm waiting for it... if it's not coming, let's do ourselves.
#endif
  return ret;
}

template <typename T>
T raw_atomic_exchange_cpp(T* target, T desired) {
  return raw_atomic_exchange_inl<T>(target, desired);
}

template <typename T>
T raw_atomic_fetch_add_cpp(T* target, T addendum) {
  return raw_atomic_fetch_add_inl<T>(target, addendum);
}

template <typename T>
T raw_atomic_load_seq_cst_cpp(const T* target) {
  return raw_atomic_load_seq_cst_inl<T>(target);
}

template <typename T>
void raw_atomic_store_seq_cst_cpp(T* target, T value) {
  raw_atomic_store_seq_cst_inl<T>(target, value);
}

template <typename T>
void raw_atomic_store_release_cpp(T* target, T value) {
  raw_atomic_store_release_inl<T>(target, value);
}

#define EXP_SWAP(x) template x raw_atomic_exchange_cpp(x *target, x desired)
INSTANTIATE_ALL_INTEGER_TYPES(EXP_SWAP);

#define EXP_FETCH_ADD(x) template x raw_atomic_fetch_add_cpp(x *target, x addendum)
INSTANTIATE_ALL_INTEGER_TYPES(EXP_FETCH_ADD);

#define EXP_LOAD_SEQ_CST(x) template x raw_atomic_load_seq_cst_cpp(const x *target)
INSTANTIATE_ALL_INTEGER_TYPES(EXP_LOAD_SEQ_CST);

#define EXP_STORE_SEQ_CST(x) template void raw_atomic_store_seq_cst_cpp(x *target, x value)
INSTANTIATE_ALL_INTEGER_TYPES(EXP_STORE_SEQ_CST);

#define EXP_STORE_RELEASE(x) template void raw_atomic_store_release_cpp(x *target, x value)
INSTANTIATE_ALL_INTEGER_TYPES(EXP_STORE_RELEASE);


}  // namespace assorted
}  // namespace foedus

