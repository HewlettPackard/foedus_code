/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_page_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <string>

namespace foedus {
namespace storage {
namespace masstree {

std::ostream& operator<<(std::ostream& o, const MasstreePage& v) {
  if (v.is_border()) {
    o << reinterpret_cast<const MasstreeBorderPage&>(v);
  } else {
    o << reinterpret_cast<const MasstreeIntermediatePage&>(v);
  }
  return o;
}

void describe_masstree_page_common(std::ostream* o_ptr, const MasstreePage& v) {
  std::ostream& o = *o_ptr;
  o << std::endl << v.header();
  o << std::endl << "<low_fence_>" << assorted::Hex(v.get_low_fence(), 16) << "</low_fence_>";
  o << "<high_fence_>" << assorted::Hex(v.get_high_fence(), 16) << "</high_fence_>";
  o << "<foster_fence_>" << assorted::Hex(v.get_foster_fence(), 16) << "</foster_fence_>";
  o << std::endl << "<foster_minor>";
  describe_volatile_pointer(&o, v.get_foster_minor());
  o << "</foster_minor>";
  o << std::endl << "<foster_major>";
  describe_volatile_pointer(&o, v.get_foster_major());
  o << "</foster_major>";
}

std::ostream& operator<<(std::ostream& o, const MasstreeIntermediatePage& v) {
  o << "<MasstreeIntermediatePage>";
  describe_masstree_page_common(&o, v);
  for (uint16_t i = 0; i <= v.get_key_count(); ++i) {
    const MasstreeIntermediatePage::MiniPage& minipage = v.get_minipage(i);
    KeySlice minipage_low = i == 0 ? v.get_low_fence() : v.get_separator(i - 1);
    o << std::endl << "  <Minipage index=\"" << static_cast<int>(i)
      << "\" low=\"" << assorted::Hex(minipage_low, 16)
      << "\" count=\"" << static_cast<int>(minipage.key_count_)
      << "\">";
    for (uint16_t j = 0; j <= minipage.key_count_; ++j) {
      o << std::endl << "    <Pointer index=\"" << static_cast<int>(j)
        << "\" low=\""
          << assorted::Hex(j == 0 ? minipage_low : minipage.separators_[j - 1], 16)
        << "\">" << minipage.pointers_[j] << "</Pointer>";
    }
    o << std::endl << "  </Minipage>";
  }
  o << "</MasstreeIntermediatePage>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const MasstreeBorderPage& v) {
  o << "<MasstreeBorderPage>";
  describe_masstree_page_common(&o, v);
  o << "<consecutive_inserts_>" << v.consecutive_inserts_ << "</consecutive_inserts_>";
  o << std::endl << "<records>";
  for (uint16_t i = 0; i < v.get_key_count(); ++i) {
    o << std::endl << "  <record index=\"" << i
      << "\" slice=\"" << assorted::Hex(v.get_slice(i), 16)
      << "\" remaining_key_len=\"" << static_cast<int>(v.get_remaining_key_length(i))
      << "\" offset=\"" << v.get_offset_in_bytes(i)
      << "\" payload_len=\"" << v.get_payload_length(i)
      << "\">";
    if (v.does_point_to_layer(i)) {
      o << "<next_layer>" << *v.get_next_layer(i) << "</next_layer>";
    } else {
      if (v.get_remaining_key_length(i) > sizeof(KeySlice)) {
        std::string suffix(v.get_record(i), v.get_remaining_key_length(i) - sizeof(KeySlice));
        o << "<key_suffix>" << assorted::HexString(suffix) << "</key_suffix>";
      }
      if (v.get_payload_length(i) > 0) {
        std::string payload(v.get_record_payload(i), v.get_payload_length(i));
        o << "<payload>" << assorted::HexString(payload) << "</payload>";
      }
    }
    o << * v.get_owner_id(i);
    o << "</record>";
  }
  o << std::endl << "</records>";
  o << "</MasstreeBorderPage>";
  return o;
}


void MasstreeBorderPage::assert_entries_impl() const {
  // the following logic holds only when this page is locked
  ASSERT_ND(header_.snapshot_ || is_locked());
  struct Sorter {
    explicit Sorter(const MasstreeBorderPage* target) : target_(target) {}
    bool operator() (uint8_t left, uint8_t right) {
      KeySlice left_slice = target_->get_slice(left);
      KeySlice right_slice = target_->get_slice(right);
      if (left_slice < right_slice) {
        return true;
      } else if (left_slice == right_slice) {
        return target_->get_remaining_key_length(left) < target_->get_remaining_key_length(right);
      } else {
        return false;
      }
    }
    const MasstreeBorderPage* target_;
  };
  uint8_t key_count = get_key_count();
  uint8_t order[kMaxKeys];
  for (uint8_t i = 0; i < key_count; ++i) {
    order[i] = i;
  }
  std::sort(order, order + key_count, Sorter(this));

  if (header_.snapshot_) {
    // in snapshot page, all entries should be fully sorted
    for (uint8_t i = 0; i < key_count; ++i) {
      ASSERT_ND(order[i] == i);
    }
  }

  for (uint8_t i = 1; i < key_count; ++i) {
    uint8_t pre = order[i - 1];
    uint8_t cur = order[i];
    ASSERT_ND(slices_[pre] <= slices_[cur]);
    if (slices_[pre] == slices_[cur]) {
      ASSERT_ND(remaining_key_length_[pre] < remaining_key_length_[cur]);
      ASSERT_ND(remaining_key_length_[pre] <= sizeof(KeySlice));
    }
  }

  // also check the padding between key suffix and payload
  for (uint8_t i = 0; i < key_count; ++i) {
    if (does_point_to_layer(i)) {
      continue;
    }
    uint16_t suffix_length = get_suffix_length(i);
    uint16_t suffix_length_aligned = get_suffix_length_aligned(i);
    if (suffix_length > 0 && suffix_length != suffix_length_aligned) {
      ASSERT_ND(suffix_length_aligned > suffix_length);
      for (uint16_t pos = suffix_length; pos < suffix_length_aligned; ++pos) {
        // must be zero-padded
        ASSERT_ND(get_record(i)[pos] == 0);
      }
    }
    if (header().snapshot_) {  // this can't be checked in volatile pages that are being changed
      uint16_t payload_length = get_payload_length(i);
      uint16_t payload_length_aligned = assorted::align8(payload_length);
      if (payload_length > 0 && payload_length != payload_length_aligned) {
        ASSERT_ND(payload_length_aligned > payload_length);
        for (uint16_t pos = payload_length; pos < payload_length_aligned; ++pos) {
          // must be zero-padded
          ASSERT_ND(get_record(i)[suffix_length_aligned + pos] == 0);
        }
      }
    }
  }
}


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
