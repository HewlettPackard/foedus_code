/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/assorted/raw_atomics.hpp"

#include <stdint.h>

#include <atomic>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace assorted {

template <typename T>
bool    raw_atomic_compare_exchange_strong(T* target, T* expected, T desired) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  // this is super ugly. but this is the only way to do it without compiler-dependent code.
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  return casted->compare_exchange_strong(*expected, desired);
}
// template explicit instantiations for all integer types.
#define EXPLICIT_INSTANTIATION_STRONG(x) \
  template bool raw_atomic_compare_exchange_strong(x *target, x *expected, x desired)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_STRONG);

#if defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
bool raw_atomic_compare_exchange_strong_uint128(
  uint64_t *ptr, const uint64_t *old_value, const uint64_t *new_value) {
  __uint128_t* ptr_casted = reinterpret_cast<__uint128_t*>(ptr);
  __uint128_t old_casted = *reinterpret_cast<const __uint128_t*>(old_value);
  __uint128_t new_casted = *reinterpret_cast<const __uint128_t*>(new_value);
  bool ret = ::__sync_bool_compare_and_swap(ptr_casted, old_casted, new_casted);
  return ret;
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
  // TODO(Hideaki) ARMv8
  // ARMv8 does have 128bit atomic instructions, called "pair" operations, such as ldaxp and stxp.
  // There is actually a library that uses it:
  // https://github.com/ivmai/libatomic_ops/blob/master/src/atomic_ops/sysdeps/gcc/aarch64.h
  // (but this is GPL. Don't open the URL unless you are ready for it.)
  // As of now (May 2014), GCC can't handle them, nor provide __uint128_t in ARMv8.
  // I think it's coming, however. I'm waiting for it... if it's not coming, let's do ourselves.
}
#endif  // defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)

template <typename T>
T raw_atomic_exchange(T* target, T desired) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  return casted->exchange(desired);
}

template <typename T>
T raw_atomic_fetch_add(T* target, T addendum) {
  static_assert(sizeof(T) == sizeof(std::atomic< T >), "std::atomic<T> size is not same as T??");
  std::atomic<T>* casted = reinterpret_cast< std::atomic<T>* >(target);
  return casted->fetch_add(addendum);
}

#define EXP_SWAP(x) template x raw_atomic_exchange(x *target, x desired)
INSTANTIATE_ALL_INTEGER_TYPES(EXP_SWAP);

#define EXP_FETCH_ADD(x) template x raw_atomic_fetch_add(x *target, x addendum)
INSTANTIATE_ALL_INTEGER_TYPES(EXP_FETCH_ADD);


}  // namespace assorted
}  // namespace foedus

