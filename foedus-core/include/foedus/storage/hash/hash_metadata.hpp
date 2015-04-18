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
#ifndef FOEDUS_STORAGE_HASH_HASH_METADATA_HPP_
#define FOEDUS_STORAGE_HASH_HASH_METADATA_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"

namespace foedus {
namespace storage {
namespace hash {
/**
 * @brief Metadata of an hash storage.
 * @ingroup HASH
 */
struct HashMetadata CXX11_FINAL : public Metadata {
  HashMetadata()
    : Metadata(0, kHashStorage, ""), bin_bits_(kHashMinBinBits), pad1_(0), pad2_(0), pad3_(0) {}
  HashMetadata(StorageId id, const StorageName& name, uint8_t bin_bits)
    : Metadata(id, kHashStorage, name), bin_bits_(bin_bits), pad1_(0), pad2_(0), pad3_(0) {
  }
  /** This one is for newly creating a storage. */
  HashMetadata(const StorageName& name, uint8_t bin_bits = kHashMinBinBits)
    : Metadata(0, kHashStorage, name), bin_bits_(bin_bits), pad1_(0), pad2_(0), pad3_(0) {
  }

  /**
   * Use this method to set an appropriate value for bin_bits_.
   * @param[in] expected_records how many records do you expect to store in this storage
   * @param[in] preferred_records_per_bin average records per a hash bin. 5-30 are recommended.
   * If this number is too large, many bins have a linked-list rather than just one page.
   */
  void      set_capacity(uint64_t expected_records, double preferred_records_per_bin = 5.0);

  /**
   * Number of bins in this hash storage. Always power of two.
   */
  uint64_t  get_bin_count() const { return 1ULL << bin_bits_; }
  /** @returns how many bits we should shift down to extract bins from hashes */
  uint8_t   get_bin_shifts() const { return 64U - bin_bits_; }
  HashBin   extract_bin(HashValue hash) const { return hash >> get_bin_shifts(); }

  std::string describe() const;
  friend std::ostream& operator<<(std::ostream& o, const HashMetadata& v);

  /**
   * Number of bins in exponent of two.
   * Recommended to use set_capacity() to set this value.
   * @invariant kHashMinBinBits <= bin_bits_ <= kHashMaxBinBits
   */
  uint8_t   bin_bits_;

  // just for valgrind when this metadata is written to file. ggr
  uint8_t   pad1_;
  uint16_t  pad2_;
  uint32_t  pad3_;
};

struct HashMetadataSerializer CXX11_FINAL : public virtual MetadataSerializer {
  HashMetadataSerializer() : MetadataSerializer() {}
  explicit HashMetadataSerializer(HashMetadata* data)
    : MetadataSerializer(data), data_casted_(data) {}
  EXTERNALIZABLE(HashMetadataSerializer);
  HashMetadata* data_casted_;
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_METADATA_HPP_
