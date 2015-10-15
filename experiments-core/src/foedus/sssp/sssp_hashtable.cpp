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
#include "foedus/sssp/sssp_hashtable.hpp"

#include <ostream>
#include <string>

namespace foedus {
namespace sssp {


DijkstraHashtable::DijkstraHashtable()
  : memory_(),
    buckets_(nullptr),
    inserted_keys_(nullptr),
    inserted_keys_count_(0),
    next_records_(nullptr),
    next_records_count_(0) {
}

ErrorCode DijkstraHashtable::create_memory(uint16_t numa_node) {
  uint64_t bucket_memory_size = sizeof(Record) * kBucketCount;
  uint64_t inserted_keys_memory_size = sizeof(Key) * kMaxRecords;
  uint64_t next_memory_size = sizeof(Record) * kMaxRecords;
  uint64_t total_memory_size = bucket_memory_size + inserted_keys_memory_size + next_memory_size;
  memory_.alloc(
    total_memory_size,
    1ULL << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node);

  char* mem = reinterpret_cast<char*>(memory_.get_block());
  uint64_t pos = 0;
  buckets_ = reinterpret_cast<Record*>(mem + pos);
  pos += bucket_memory_size;
  inserted_keys_ = reinterpret_cast<Key*>(mem + pos);
  pos += inserted_keys_memory_size;
  next_records_ = reinterpret_cast<Record*>(mem + pos);
  pos += next_memory_size;
  ASSERT_ND(pos == total_memory_size);

  inserted_keys_count_ = 0;
  next_records_count_ = 0;
  return kErrorCodeOk;
}

void DijkstraHashtable::release_memory() {
  memory_.release_block();
  buckets_ = nullptr;
  inserted_keys_ = nullptr;
  next_records_ = nullptr;
  inserted_keys_count_ = 0;
  next_records_count_ = 0;
}

void DijkstraHashtable::clean() {
  for (uint32_t i = 0; i < inserted_keys_count_; ++i) {
    Key key = inserted_keys_[i];
    uint16_t b = hashinate(key);
    buckets_[b].init(0);
  }
  inserted_keys_count_ = 0;
  next_records_count_ = 0;
}

std::ostream& operator<<(std::ostream& o, const DijkstraHashtable& v) {
  // Each bin shouldn't have that many records... so, output everything!
  o << "<DijkstraHashtable>" << std::endl;
  o << "  " << v.memory_ << std::endl;
  o << "  <inserted_keys_count_>"
    << v.inserted_keys_count_ << "</inserted_keys_count_>" << std::endl;
  o << "  <next_records_count_>" << v.next_records_count_ << "</next_records_count_>" << std::endl;
  if (v.buckets_) {
    o << "  <buckets_>" << std::endl;
    for (uint32_t i = 0; i < DijkstraHashtable::kBucketCount; ++i) {
      if (v.buckets_[i].is_used()) {
        o << "    <bucket idx=\"" << i << "\">";
        o << "<key>" << v.buckets_[i].key_ << "</key>";
        o << "<next>" << v.buckets_[i].next_ << "</next>";
        o << "<value_distance>" << v.buckets_[i].value_.distance_ << "</value_distance>";
        o << "<value_previous>" << v.buckets_[i].value_.previous_ << "</value_previous>";
        o << "</bucket>";
      }
    }
    o << "  </buckets_>" << std::endl;
  }
  o << "</DijkstraHashtable>";
  return o;
}


}  // namespace sssp
}  // namespace foedus
