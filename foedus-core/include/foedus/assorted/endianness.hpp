/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_ENDIANNESS_HPP_
#define FOEDUS_ASSORTED_ENDIANNESS_HPP_

#include <endian.h>
#include <stdint.h>

#include <limits>

#include "foedus/compiler.hpp"

/**
 * @file foedus/assorted/endianness.hpp
 * @ingroup ASSORTED
 * @brief A few macros and helper methods related to byte endian-ness.
 * @details
 * Big endian is preferred in places where we need memcmp correctly work.
 * OTH, little endinan is preferred where the host machine is little endian and we want to
 * do native integer comparison for better performance.
 *
 * As a result, we use both in our code. Hence, we need to:
 *  \li detect whether the host is big endian or little endian
 *  \li convert big-endian to/from host-endian (which is probably little-endian).
 */

namespace foedus {
namespace assorted {

/**
 * @brief A handy const boolean to tell if it's little endina.
 * @ingroup ASSORTED
 * @details
 * Most compilers would resolve "if (kIsLittleEndian) ..." at compile time, so no overhead.
 * Compared to writing ifdef each time, this is handier.
 * However, there are a few cases where we have to write ifdef (class definition etc).
 */
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
const bool kIsLittleEndian = false;
#elif __BYTE_ORDER == __BIG_ENDIAN
const bool kIsLittleEndian = true;
#else  // __BYTE_ORDER == __BIG_ENDIAN
#error "__BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN."
#endif  // __BYTE_ORDER


// same as bswap, but this is standard, so let's prefer that.
// however, remember these are macros. we can't say "::be64toh"
template <typename T> T betoh(T be_value);
template <> inline uint64_t betoh<uint64_t>(uint64_t be_value) { return be64toh(be_value); }
template <> inline uint32_t betoh<uint32_t>(uint32_t be_value) { return be32toh(be_value); }
template <> inline uint16_t betoh<uint16_t>(uint16_t be_value) { return be16toh(be_value); }
template <> inline uint8_t betoh<uint8_t>(uint8_t be_value) { return be_value; }

// the following is much simpler if we assume std::make_unsigned< I >, but that's C++11.
// we shouldn't use it in a public header.
// also, we shouldn't abuse template meta programming.
template <> inline int64_t betoh<int64_t>(int64_t be_value) {
  return be64toh(static_cast<uint64_t>(be_value)) - (1ULL << 63);
}
template <> inline int32_t betoh<int32_t>(int32_t be_value) {
  return be32toh(static_cast<uint32_t>(be_value)) - (1U << 31);
}
template <> inline int16_t betoh<int16_t>(int16_t be_value) {
  return be16toh(static_cast<uint16_t>(be_value)) - (1U << 15);
}
template <> inline int8_t betoh<int8_t>(int8_t be_value) {
  return be_value - (1U << 7);
}

template <typename T> T htobe(T host_value);
template <> inline uint64_t htobe<uint64_t>(uint64_t host_value) { return htobe64(host_value); }
template <> inline uint32_t htobe<uint32_t>(uint32_t host_value) { return htobe32(host_value); }
template <> inline uint16_t htobe<uint16_t>(uint16_t host_value) { return htobe16(host_value); }
template <> inline uint8_t htobe<uint8_t>(uint8_t host_value) { return host_value; }

template <> inline int64_t htobe<int64_t>(int64_t host_value) {
  return htobe64(static_cast<uint64_t>(host_value) - (1ULL << 63));
}
template <> inline int32_t htobe<int32_t>(int32_t host_value) {
  return htobe32(static_cast<uint32_t>(host_value) - (1U << 31));
}
template <> inline int16_t htobe<int16_t>(int16_t host_value) {
  return htobe16(static_cast<uint16_t>(host_value) - (1U << 15));
}
template <> inline int8_t htobe<int8_t>(int8_t host_value) {
  return host_value - (1U << 7);
}

/**
 * @brief Convert a big-endian byte array to a native integer.
 * @param[in] be_bytes a big-endian byte array. MUST BE ALIGNED.
 * @return converted native integer
 * @tparam T type of native integer
 * @ingroup ASSORTED
 * @details
 * Almost same as bexxtoh in endian.h except this is a single template function that supports
 * signed integers.
 */
template <typename T>
inline T read_bigendian(const void* be_bytes) {
  // all if clauses below will be elimiated by compiler.
  const T* be_address = reinterpret_cast<const T*>(ASSUME_ALIGNED(be_bytes, sizeof(T)));
  T be_value = *be_address;
  return betoh(be_value);
}

/**
 * @brief Convert a native integer to big-endian bytes and write them to the given address.
 * @param[in] host_value a native integer.
 * @param[out] be_bytes address to write out big endian bytes. MUST BE ALIGNED.
 * @tparam T type of native integer
 * @ingroup ASSORTED
 */
template <typename T>
inline void write_bigendian(T host_value, void* be_bytes) {
  T* be_address = reinterpret_cast<T*>(ASSUME_ALIGNED(be_bytes, sizeof(T)));
  *be_address = htobe<T>(host_value);
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_ENDIANNESS_HPP_
