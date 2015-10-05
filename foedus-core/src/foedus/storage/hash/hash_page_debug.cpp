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
#include "foedus/storage/hash/hash_page_impl.hpp"

#include <ostream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_id.hpp"

namespace foedus {
namespace storage {
namespace hash {

std::ostream& operator<<(std::ostream& o, const HashIntermediatePage& v) {
  o << "<HashIntermediatePage>";
  o << std::endl << "  <vaddr>" << assorted::Hex(reinterpret_cast<uintptr_t>(&v), 16) << "</vaddr>";
  o << std::endl << "  " << v.header();
  o << std::endl << "  <level>" << static_cast<int>(v.get_level()) << "</level>";
  o << std::endl << "  " << v.get_bin_range();
  HashBin total_begin = v.get_bin_range().begin_;
  HashBin interval = kHashMaxBins[v.get_level()];
  for (uint16_t i = 0; i < kHashIntermediatePageFanout; ++i) {
    DualPagePointer pointer = v.get_pointer(i);
    if (pointer.is_both_null()) {
      continue;
    }
    o << std::endl << "  <Pointer index=\"" << static_cast<int>(i)
      << "\" bin_begin=\"" << (total_begin + i * interval)
      << "\">" << pointer << "</Pointer>";
  }
  o << "</HashIntermediatePage>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const HashDataPage& v) {
  o << "<HashDataPage>";
  o << std::endl << "  <vaddr>" << assorted::Hex(reinterpret_cast<uintptr_t>(&v), 16) << "</vaddr>";
  o << std::endl << v.header();
  o << std::endl << "  <bin>" << assorted::Hex(v.get_bin()) << "</bin>";
  uint16_t records = v.get_record_count();
  o << std::endl << "  <record_count>" << records << "</record_count>";
  o << std::endl << "  <next_page>" << v.next_page() << "</next_page>";
  o << std::endl << "  <records>";
  for (DataPageSlotIndex i = 0; i < records; ++i) {
    const HashDataPage::Slot* slot = v.get_slot_address(i);
    o << std::endl << "  <record index=\"" << i
      << "\" hash=\"" << assorted::Hex(slot->hash_, 16)
      << "\" physical_record_len=\"" << slot->physical_record_length_
      << "\" key_length_=\"" << slot->key_length_
      << "\" offset=\"" << slot->offset_
      << "\" payload_len=\"" << slot->payload_length_
      << "\">";
    const char* data = v.record_from_offset(slot->offset_);
    std::string key(data, slot->key_length_);
    o << "<key>" << assorted::HexString(key) << "</key>";
    if (slot->payload_length_ > 0) {
      std::string payload(data + slot->get_aligned_key_length(), slot->payload_length_);
      o << "<payload>" << assorted::HexString(payload) << "</payload>";
    }
    BloomFilterFingerprint fingerprint = DataPageBloomFilter::extract_fingerprint(slot->hash_);
    o << fingerprint;
    o << slot->tid_;
    o << "</record>";
  }
  o << std::endl << "  </records>";
  o << std::endl << "  <BloomFilter>" << v.bloom_filter_ << "</BloomFilter>";
  o << "</HashDataPage>";
  return o;
}


void HashDataPage::assert_entries_impl() const {
  const uint8_t bin_shifts = get_bin_shifts();
  uint8_t records = get_record_count();

  if (header_.snapshot_) {
    ASSERT_ND(next_page().volatile_pointer_.is_null());
  } else {
    ASSERT_ND(next_page().snapshot_pointer_ == 0);
  }

  DataPageBloomFilter correct_filter;
  correct_filter.clear();
  for (DataPageSlotIndex i = 0; i < records; ++i) {
    const HashDataPage::Slot* slot = get_slot_address(i);
    if (i > 0) {
      const HashDataPage::Slot* pre = get_slot_address(i - 1);
      ASSERT_ND(slot->offset_ == pre->offset_ + pre->physical_record_length_);
    }
    HashValue hash = hashinate(record_from_offset(slot->offset_), slot->key_length_);
    ASSERT_ND(slot->hash_ == hash);
    HashBin bin = hash >> bin_shifts;
    ASSERT_ND(bin_ == bin);

    correct_filter.add(DataPageBloomFilter::extract_fingerprint(slot->hash_));
    ASSERT_ND(slot->physical_record_length_
      >= slot->get_aligned_key_length() + slot->payload_length_);
    uint16_t end_offset = slot->offset_ + slot->physical_record_length_;
    ASSERT_ND(end_offset + records * sizeof(Slot) <= (kPageSize - kHashDataPageHeaderSize));
  }

  for (uint16_t i = 0; i < sizeof(correct_filter.values_); ++i) {
    ASSERT_ND(correct_filter.values_[i] == bloom_filter_.values_[i]);
  }
}


}  // namespace hash
}  // namespace storage
}  // namespace foedus
