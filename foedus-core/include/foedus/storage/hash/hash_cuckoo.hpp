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
#ifndef FOEDUS_STORAGE_HASH_CUCKOO_HPP_
#define FOEDUS_STORAGE_HASH_CUCKOO_HPP_

#include <stdint.h>

#include <cstring>
#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"

/**
 * @file foedus/storage/hash/hash_cuckoo.hpp
 * @brief Independent utility classes/methods used in our cuckoo hashing implementation.
 * @ingroup HASH
 */
namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief Good old multiply-add hashination.
 * @ingroup HASH
 */
const uint64_t kHashinateMultiplier(0xB7CE1A1232D20E87ULL);

/**
 * @brief To get alternative bin, we generate random bit patterns with tag value.
 * @ingroup HASH
 * @details
 * 256 patterns for 1 byte. Tag is 2 bytes, so high and low word respectively.
 */
const uint32_t kTagRandomizers[] = {
  0xb7024e47U, 0xeb7a2910U, 0xa4f39cb4U, 0x29f4bca1U,
  0x32dcb014U, 0x22859a9eU, 0x173b4868U, 0x9fe47246U,
  0x6bf7b359U, 0xf5085e4bU, 0xebcf4867U, 0xcdc838b7U,
  0x7e359ef3U, 0x2ebdd537U, 0x4f5bacb8U, 0xf5cb2bc5U,
  0x3bc355f2U, 0x1265f3b9U, 0xaaad6abcU, 0xe1444782U,
  0xfd849cf0U, 0xbac0263eU, 0x13f87255U, 0x1b451335U,
  0xd251d5eeU, 0xa1b8a115U, 0xb3cfa25bU, 0x7b45bdfeU,
  0xa4c81691U, 0x96c79868U, 0x307843e2U, 0xa16b0395U,
  0xa9d7bf77U, 0x168fb181U, 0x0a9bb708U, 0x886582e5U,
  0xdc9ecd93U, 0x2dbbabf8U, 0xa6da6222U, 0xf18515fbU,
  0x564e800aU, 0x2faaac5dU, 0x49e338a6U, 0x333a6d17U,
  0x009de02bU, 0xed625c4aU, 0xafe3facdU, 0x3a19af11U,
  0x870bfe69U, 0x848eae93U, 0xd56ac57cU, 0xa0cbb388U,
  0x4344d925U, 0x52de6d9eU, 0x5c7b0e41U, 0xe96592c5U,
  0x26f2e554U, 0x00f74697U, 0x09309a07U, 0x3669b871U,
  0x057cd7ddU, 0x6d5c056dU, 0x21e848b3U, 0xa02a7cbbU,
  0xfbf781a4U, 0x3c388186U, 0x438a850cU, 0x3ac52538U,
  0xb20c9a54U, 0x752d1c12U, 0xaf088b7cU, 0x55479816U,
  0x2c9b31a2U, 0x529e368cU, 0x049bcef2U, 0xcb724de6U,
  0x02c0a2f4U, 0xfaeaa7caU, 0x7136baacU, 0x3083af21U,
  0xa7103de6U, 0x0d7d5448U, 0xa90ec6dfU, 0x1ada07caU,
  0xe4b1115cU, 0xb37c5adeU, 0xccd6d23fU, 0x107e8894U,
  0x35eeaeeaU, 0xc145725fU, 0x9c4094f0U, 0xcb3ff6d2U,
  0x2b3df9fcU, 0xf8551f71U, 0x5910b77dU, 0x36e5639aU,
  0x4a829a9eU, 0x15c2e37eU, 0x182dea0eU, 0x4b31cab1U,
  0xd81ed306U, 0x4951d93fU, 0xdec5ee3cU, 0x6394c17fU,
  0x1d8c244dU, 0xe14e643dU, 0xe6df9e70U, 0x17de5631U,
  0x534afc51U, 0x9b819b9dU, 0xd8b9c0a4U, 0x0daa8321U,
  0x71932cb0U, 0x06557dd4U, 0x23ad63f8U, 0x841641faU,
  0x1ee26f32U, 0xcd2a2310U, 0x0952d80eU, 0x55898241U,
  0x6c73e35dU, 0xf3b65e14U, 0x4bb883bbU, 0xda200dd3U,
  0xea79a2bbU, 0x11569e77U, 0x7236f657U, 0x88e29fa2U,
  0x42e81928U, 0x66121b17U, 0xdfee5f3cU, 0x5c331a6eU,
  0x21e44a25U, 0x3646e1c4U, 0xab8595e8U, 0x8038193eU,
  0x00135ddaU, 0x56683f5bU, 0x41314d94U, 0x69cced3eU,
  0xd2477007U, 0x99509031U, 0xa2a55c20U, 0xac759e87U,
  0xf4da29bbU, 0x16fa89e4U, 0x8fa3456aU, 0xe7055184U,
  0x84153ed2U, 0x6a5797abU, 0x0ec60bbdU, 0x6ec0be87U,
  0xa3d7bab6U, 0x8da30863U, 0x065f7f5fU, 0xfa11f9d6U,
  0x16718e51U, 0x254414b7U, 0xb6760c00U, 0xa2ed57ffU,
  0x37b160a5U, 0x02505f2dU, 0x9c72e743U, 0x77070e01U,
  0xbe9d85c3U, 0x22e8be7cU, 0xe2a9b9b1U, 0x26ee4ab7U,
  0x03653610U, 0xb14f5bb4U, 0xa7475bcfU, 0x021ae3c5U,
  0x21b4c290U, 0xa5f2af2aU, 0xea169ff4U, 0xce767361U,
  0x5d005e21U, 0x2ecb04a8U, 0x94d349f9U, 0x8373ca48U,
  0xa10bcd76U, 0x0cddea18U, 0xef10dcebU, 0xe95c9175U,
  0x86924849U, 0x13fac277U, 0xdf7a5e7aU, 0xfc4569deU,
  0xe95b9653U, 0x4acd700eU, 0x6c4cd17eU, 0x56391922U,
  0x7b2e08adU, 0xbadcef28U, 0x5b535c9bU, 0xf579b449U,
  0xebfd6c44U, 0xfa2cf2c9U, 0x18415b9eU, 0x8de077ceU,
  0xcf69623fU, 0xf427d887U, 0xebf2fe7cU, 0x4ffdf8e2U,
  0x19818c43U, 0xbe822f4dU, 0x471a2ff0U, 0xc3c7cff7U,
  0xb816d943U, 0x66e799beU, 0x953ccf01U, 0x9029706aU,
  0x446a67b6U, 0x7111514bU, 0x476ca1caU, 0xf2940166U,
  0x2a6077d2U, 0xa3b07c0eU, 0x1fa610edU, 0xe9d4b384U,
  0xcf69b46cU, 0xe584426aU, 0x6b699b5dU, 0xd8b4def2U,
  0xfcd545aeU, 0x26c0f897U, 0xef468c3fU, 0x9468b693U,
  0xa0f04183U, 0x93eb73d9U, 0x7ab54b57U, 0x2a157b6dU,
  0xf899c11eU, 0x717123caU, 0x7c3afe6bU, 0x0e8b2b1fU,
  0x9b132531U, 0x6462aa94U, 0xedaaf99eU, 0x1342fa1dU,
  0x4f494339U, 0x56b6ec0cU, 0x4b6ced22U, 0x22a0532cU,
  0xdbd36e0cU, 0xb29a8f15U, 0xa1745936U, 0xbdca93beU,
  0x0cb336d5U, 0x1896af2dU, 0x1faf3f34U, 0x19c3f2efU,
  0x90c34e43U, 0x0d6b6f4fU, 0x91015460U, 0x6131a347U,
};

/**
 * @brief A package of hash values and bins.
 * @ingroup HASH
 * @details
 * Cuckoo hashing in [FAN13] calculates one bin and one tag for each key.
 * We do similar, but there is a slight alternation to allow arbitrary bin count.
 * This objects packages all computed values to easily pass around.
 * This object is a POD.
 */
struct HashCombo {
  HashCombo(const void *key, uint16_t key_length, uint8_t bin_bits);

  template <typename T>
  HashCombo(T key, uint8_t bin_bits);

  void init(uint64_t hash, HashTag tag, uint8_t bin_bits);

  friend std::ostream& operator<<(std::ostream& o, const HashCombo& v);

  uint64_t  hash_;
  uint64_t  bins_[2];
  HashTag   tag_;
  uint8_t   bin_bits_;

  // the followings are set in lookup_bin method

  /** Only when record_ is set. */
  uint8_t       record_slot_;
  /** Only when record_ is set. */
  uint16_t      payload_length_;
  /** Sets only when record_ is set. Whether it came from bins_[0] (bin1, oh, 0 base) or not.  */
  bool          record_bin1_;
  /** We copy this BEFORE checking tags. */
  uint16_t      observed_mod_count_[2];
  /** Bitmap of tags that hit. kMaxEntriesPerBin bits < 32bit */
  uint32_t      hit_bitmap_[2];
  /** The bin page. null if no page matching. */
  HashBinPage*  bin_pages_[2];
  /** The data page. null if no page matching. */
  HashDataPage* data_pages_[2];

  // following are set in locate_record()

  /** The exact record. null if didn't find the exact record. Obviously just one, not two. */
  Record*       record_;
};

/**
 * @brief Calculates hash value for general input.
 * @ingroup HASH
 */
inline uint64_t hashinate(const void *key, uint16_t key_length) {
  uint64_t result = 0;
  // treat each 8 bytes up to the last 8- bytes for better performance.
  const uint64_t *casted = reinterpret_cast<const uint64_t*>(key);
  for (uint16_t i = 0; i < (key_length >> 3U); ++i) {
    result = result * kHashinateMultiplier + casted[i];
  }
  uint16_t consumed = (key_length >> 3U) << 3U;
  uint8_t remaining = key_length - consumed;
  if (remaining) {
    uint64_t tmp = 0;
    std::memcpy(&tmp, reinterpret_cast<const char*>(key) + consumed, remaining);
    result = result * kHashinateMultiplier + tmp;
  }

  return result;
}

/**
 * @brief Modified tag value by consuming one 64bit value.
 * @ingroup HASH
 */
inline HashTag tag_consume(HashTag current, uint64_t slice) {
  return current ^
    static_cast<uint16_t>(slice >> 48) ^
    static_cast<uint16_t>(slice >> 32) ^
    static_cast<uint16_t>(slice >> 16) ^
    static_cast<uint16_t>(slice);
}

/**
 * @brief Generate an integer as randomizer from the tag.
 * @ingroup HASH
 */
inline uint64_t tag_to_randomizer(HashTag tag) {
  return (static_cast<uint64_t>(kTagRandomizers[static_cast<uint8_t>(tag >> 8)]) << 32) |
    kTagRandomizers[static_cast<uint8_t>(tag)];
}

/**
 * @brief Fold (compress) bits in the given 64bit value to make it within 2^bin_bits.
 * @ingroup HASH
 */
inline uint64_t fold_bits(uint64_t value, uint8_t bin_bits) {
  ASSERT_ND(bin_bits >= 8);
  uint8_t shift = 64 - bin_bits;
  if (shift <= 32) {
    value = (value >> shift) ^ (value & 0xFFFFFFFFU);
  } else {
    value = (value >> 32) ^ (value & 0xFFFFFFFFU);
    shift -= 32;
    if (shift <= 16) {
      value = (value >> shift) ^ (value & 0xFFFFU);
    } else {
      value = (value >> 16) ^ (value & 0xFFFFU);
      shift -= 16;
      ASSERT_ND(shift <= 8);  // because of the invariant of bin_bits>=8
      value = (value >> shift) ^ (value & 0xFFU);
    }
  }
  ASSERT_ND(value < (1ULL << bin_bits));
  return value;
}

/**
 * @brief Calculates hash value and tag for general input at once.
 * @ingroup HASH
 */
inline uint64_t hashinate_combo(const void *key, uint16_t key_length, HashTag *tag) {
  uint64_t result = 0;
  HashTag tag_result = 0;
  // treat each 8 bytes up to the last 8- bytes for better performance.
  const uint64_t *casted = reinterpret_cast<const uint64_t*>(key);
  for (uint16_t i = 0; i < (key_length >> 3U); ++i) {
    uint64_t slice = casted[i];
    result = result * kHashinateMultiplier + slice;
    tag_result = tag_consume(tag_result, slice);
  }
  uint16_t consumed = (key_length >> 3U) << 3U;
  uint8_t remaining = key_length - consumed;
  if (remaining) {
    uint64_t tmp = 0;
    std::memcpy(&tmp, reinterpret_cast<const char*>(key) + consumed, remaining);
    result = result * kHashinateMultiplier + tmp;
    tag_result = tag_consume(tag_result, tmp);
  }

  *tag = tag_result;
  return result;
}

/**
 * @brief Calculates hash value for a primitive type.
 * @param[in] key Primitive key to hash
 * @tparam T Primitive type. Must be within 8 bytes (don't give int128_t).
 * @ingroup HASH
 */
template <typename T>
inline uint64_t hashinate(T key) {
  // we must do this because it might be little/big endian.
  // alternatively, add ifdef and bit shift. but I hope compiler is smart enough to
  // convert this to a bit shift.
  uint64_t tmp = 0;
  std::memcpy(&tmp, &key, sizeof(T));
  return tmp;
}

// for exact sizes, we don't need memcpy
template <>
inline uint64_t hashinate<uint64_t>(uint64_t key) { return key; }

template <>
inline uint64_t hashinate<int64_t>(int64_t key) { return static_cast<uint64_t>(key); }

/**
 * @brief Calculates hash value and tag for a primitive type at once.
 * @param[in] key Primitive key to hash
 * @tparam T Primitive type. Must be within 8 bytes (don't give int128_t).
 * @ingroup HASH
 */
template <typename T>
inline uint64_t hashinate_combo(T key, HashTag *tag) {
  uint64_t tmp = 0;
  std::memcpy(&tmp, &key, sizeof(T));
  *tag = tag_consume(0, tmp);
  return tmp;
}

template <>
inline uint64_t hashinate_combo<uint64_t>(uint64_t key, HashTag *tag) {
  *tag = tag_consume(0, key);
  return key;
}

template <>
inline uint64_t hashinate_combo<int64_t>(int64_t key, HashTag *tag) {
  *tag = tag_consume(0, static_cast<uint64_t>(key));
  return static_cast<uint64_t>(key);
}

inline HashCombo::HashCombo(const void* key, uint16_t key_length, uint8_t bin_bits) {
  HashTag tmp_tag;
  uint64_t tmp_hash = hashinate_combo(key, key_length, &tmp_tag);
  init(tmp_hash, tmp_tag, bin_bits);
}

template <typename T>
inline HashCombo::HashCombo(T key, uint8_t bin_bits) {
  HashTag tmp_tag;
  uint64_t tmp_hash = hashinate_combo(key, &tmp_tag);
  init(tmp_hash, tmp_tag, bin_bits);
}

inline void HashCombo::init(uint64_t hash, HashTag tag, uint8_t bin_bits) {
  hash_ = hash;
  tag_ = tag;
  bin_bits_ = bin_bits;
  uint64_t reversed = __builtin_bswap64(hash_);  // TASK(Hideaki) non-GCC
  bins_[0] = fold_bits(reversed, bin_bits);
  bins_[1] = bins_[0] ^ fold_bits(tag_to_randomizer(tag_), bin_bits);

  payload_length_ = 0;
  record_bin1_ = false;
  observed_mod_count_[0] = 0;
  observed_mod_count_[1] = 0;
  hit_bitmap_[0] = 0;
  hit_bitmap_[1] = 0;
  bin_pages_[0] = CXX11_NULLPTR;
  bin_pages_[1] = CXX11_NULLPTR;
  data_pages_[0] = CXX11_NULLPTR;
  data_pages_[1] = CXX11_NULLPTR;
  record_ = CXX11_NULLPTR;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_CUCKOO_HPP_
