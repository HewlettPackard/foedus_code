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
#include "foedus/storage/hash/hash_tmpbin.hpp"

#include <glog/logging.h>

#include <ostream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace storage {
namespace hash {


HashTmpBin::HashTmpBin()
  : memory_(), records_capacity_(0), records_consumed_(0), buckets_(nullptr), records_(nullptr) {
}

ErrorCode HashTmpBin::create_memory(uint16_t numa_node, uint64_t initial_size) {
  ASSERT_ND(initial_size > sizeof(RecordIndex) * kBucketCount);
  ASSERT_ND(sizeof(Record) == kPageSize);
  memory_.alloc(
    initial_size,
    1ULL << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node);
  on_memory_set();
  clean();
  return kErrorCodeOk;
}

void HashTmpBin::release_memory() {
  memory_.release_block();
  on_memory_set();
  clean();
}

void HashTmpBin::steal_memory(memory::AlignedMemory* provider) {
  ASSERT_ND(!provider->is_null());
  memory_ = std::move(*provider);
  ASSERT_ND(provider->is_null());
  ASSERT_ND(!memory_.is_null());
  on_memory_set();
  clean();
}

void HashTmpBin::give_memory(memory::AlignedMemory* recipient) {
  ASSERT_ND(!memory_.is_null());
  *recipient = std::move(memory_);
  ASSERT_ND(!recipient->is_null());
  ASSERT_ND(memory_.is_null());
  on_memory_set();
  clean();
}


void HashTmpBin::clean() {
  records_consumed_ = get_first_record();
  if (buckets_) {
    std::memset(buckets_, 0, records_consumed_ * sizeof(Record));
  }
}

void HashTmpBin::clean_quick() {
  ASSERT_ND(buckets_);
  ASSERT_ND(records_capacity_ > 0);
  ASSERT_ND(records_consumed_ >= get_first_record());
  // at some point, just mem-zero is faster. we could do a switch like below, but it's rare.
  // this also makes unit-testing more tricky.
  // if (records_consumed_ > get_first_record() + 256U) {
  //   clean();
  //   return;
  // }
  for (RecordIndex index = get_first_record(); index < records_consumed_; ++index) {
    Record* record = get_record(index);
    buckets_[extract_bucket(record->hash_)] = 0;
  }
  records_consumed_ = get_first_record();
}


void HashTmpBin::on_memory_set() {
  if (memory_.is_null()) {
    buckets_ = nullptr;
    records_ = nullptr;
    records_capacity_ = 0;
  } else {
    buckets_ = reinterpret_cast<RecordIndex*>(memory_.get_block());
    records_ = reinterpret_cast<Record*>(memory_.get_block());
    records_capacity_ = memory_.get_size() / sizeof(Record);
    ASSERT_ND(sizeof(RecordIndex) * kBucketCount % sizeof(Record) == 0);
  }
}

inline ErrorCode HashTmpBin::alloc_record(RecordIndex* out) {
  if (UNLIKELY(records_consumed_ == records_capacity_)) {
    // expand memory with keeping the content, which is expensive! this mustn't happen often
    LOG(INFO) << "We need to resize HashTmpBin! current_size=" << memory_.get_size();
    CHECK_ERROR_CODE(memory_.assure_capacity(memory_.get_size() * 2, 2.0, true));
    on_memory_set();
  }

  ASSERT_ND(records_consumed_ < records_capacity_);
  *out = records_consumed_;
  ++records_consumed_;
  return kErrorCodeOk;
}

ErrorCode HashTmpBin::insert_record(
  xct::XctId xct_id,
  const void* key,
  uint16_t key_length,
  HashValue hash,
  const void* payload,
  uint16_t payload_length) {
  ASSERT_ND(!xct_id.is_deleted());
  ASSERT_ND(hashinate(key, key_length) == hash);
  SearchResult result = search_bucket(key, key_length, hash);
  if (result.found_ == 0) {
    RecordIndex new_index;
    CHECK_ERROR_CODE(alloc_record(&new_index));
    Record* record = get_record(new_index);
    record->set_all(xct_id, key, key_length, hash, payload, payload_length);

    if (result.tail_ == 0) {
      uint16_t bucket_index = extract_bucket(hash);
      ASSERT_ND(buckets_[bucket_index] == 0);
      buckets_[bucket_index] = new_index;
    } else {
      ASSERT_ND(buckets_[extract_bucket(hash)] != 0);
      Record* tail_record = get_record(result.tail_);
      tail_record->next_ = new_index;
    }
  } else {
    Record* record = get_record(result.found_);
    ASSERT_ND(record->hash_ == hash);
    if (UNLIKELY(!record->xct_id_.is_deleted())) {
      DLOG(WARNING) << "HashTmpBin::insert_record() hit KeyAlreadyExists case. This must not"
        << " happen except unit testcases.";
      return kErrorCodeStrKeyAlreadyExists;
    }
    ASSERT_ND(record->xct_id_.compare_epoch_and_orginal(xct_id) < 0);
    record->xct_id_ = xct_id;
    record->set_payload(payload, payload_length);
  }

  return kErrorCodeOk;
}

ErrorCode HashTmpBin::delete_record(
  xct::XctId xct_id,
  const void* key,
  uint16_t key_length,
  HashValue hash) {
  ASSERT_ND(xct_id.is_deleted());
  ASSERT_ND(hashinate(key, key_length) == hash);
  SearchResult result = search_bucket(key, key_length, hash);
  if (UNLIKELY(result.found_ == 0)) {
    DLOG(WARNING) << "HashTmpBin::delete_record() hit KeyNotFound case 1. This must not"
      << " happen except unit testcases.";
    return kErrorCodeStrKeyNotFound;
  } else {
    Record* record = get_record(result.found_);
    ASSERT_ND(record->hash_ == hash);
    if (UNLIKELY(record->xct_id_.is_deleted())) {
      DLOG(WARNING) << "HashTmpBin::delete_record() hit KeyNotFound case 2. This must not"
        << " happen except unit testcases.";
      return kErrorCodeStrKeyNotFound;
    }
    ASSERT_ND(record->xct_id_.compare_epoch_and_orginal(xct_id) < 0);
    record->xct_id_ = xct_id;
  }

  return kErrorCodeOk;
}

ErrorCode HashTmpBin::overwrite_record(
  xct::XctId xct_id,
  const void* key,
  uint16_t key_length,
  HashValue hash,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  ASSERT_ND(!xct_id.is_deleted());
  ASSERT_ND(hashinate(key, key_length) == hash);
  SearchResult result = search_bucket(key, key_length, hash);
  if (UNLIKELY(result.found_ == 0)) {
    DLOG(WARNING) << "HashTmpBin::overwrite_record() hit KeyNotFound case 1. This must not"
      << " happen except unit testcases.";
    return kErrorCodeStrKeyNotFound;
  } else {
    Record* record = get_record(result.found_);
    ASSERT_ND(record->hash_ == hash);
    if (UNLIKELY(record->xct_id_.is_deleted())) {
      DLOG(WARNING) << "HashTmpBin::overwrite_record() hit KeyNotFound case 2. This must not"
        << " happen except unit testcases.";
      return kErrorCodeStrKeyNotFound;
    } else if (UNLIKELY(record->payload_length_ < payload_offset + payload_count)) {
      DLOG(WARNING) << "HashTmpBin::overwrite_record() hit TooShortPayload case. This must not"
        << " happen except unit testcases.";
      return kErrorCodeStrTooShortPayload;
    }
    ASSERT_ND(record->xct_id_.compare_epoch_and_orginal(xct_id) < 0);
    record->xct_id_ = xct_id;
    record->overwrite_payload(payload, payload_offset, payload_count);
  }

  return kErrorCodeOk;
}

ErrorCode HashTmpBin::update_record(
  xct::XctId xct_id,
  const void* key,
  uint16_t key_length,
  HashValue hash,
  const void* payload,
  uint16_t payload_length) {
  ASSERT_ND(!xct_id.is_deleted());
  ASSERT_ND(hashinate(key, key_length) == hash);
  SearchResult result = search_bucket(key, key_length, hash);
  if (UNLIKELY(result.found_ == 0)) {
    DLOG(WARNING) << "HashTmpBin::update_record() hit KeyNotFound case 1. This must not"
      << " happen except unit testcases.";
    return kErrorCodeStrKeyNotFound;
  } else {
    Record* record = get_record(result.found_);
    ASSERT_ND(record->hash_ == hash);
    if (UNLIKELY(record->xct_id_.is_deleted())) {
      DLOG(WARNING) << "HashTmpBin::update_record() hit KeyNotFound case 2. This must not"
        << " happen except unit testcases.";
      return kErrorCodeStrKeyNotFound;
    }
    ASSERT_ND(record->xct_id_.compare_epoch_and_orginal(xct_id) < 0);
    record->xct_id_ = xct_id;
    record->set_payload(payload, payload_length);
  }

  return kErrorCodeOk;
}


inline HashTmpBin::SearchResult HashTmpBin::search_bucket(
  const void* key,
  uint16_t key_length,
  HashValue hash) const {
  uint16_t bucket_index = extract_bucket(hash);
  RecordIndex head = buckets_[bucket_index];
  if (head == 0) {
    return SearchResult(0, 0);
  }

  ASSERT_ND(head > 0);
  RecordIndex last_seen = 0;
  for (RecordIndex cur = head; cur != 0;) {
    Record* record = get_record(cur);
    if (record->hash_ == hash && record->key_length_ == key_length) {
      if (LIKELY(std::memcmp(record->get_key(), key, key_length) == 0)) {
        return SearchResult(cur, 0);
      }
    }

    last_seen = cur;
    cur = record->next_;
  }

  return SearchResult(0, last_seen);
}

std::ostream& operator<<(std::ostream& o, const HashTmpBin& v) {
  // Each bin shouldn't have that many records... so, output everything!
  o << "<HashTmpBin>" << std::endl;
  o << "  " << v.memory_ << std::endl;
  o << "  <records_capacity_>" << v.records_capacity_ << "</records_capacity_>" << std::endl;
  o << "  <records_consumed_>" << v.records_consumed_ << "</records_consumed_>" << std::endl;
  o << "  <buckets_>" << std::endl;
  for (uint32_t i = 0; i < HashTmpBin::kBucketCount; ++i) {
    if (v.buckets_[i] != 0) {
      o << "    <bucket idx=\"" << i << "\" head_rec=\"" << v.buckets_[i] << "\" />" << std::endl;
    }
  }
  o << "  </buckets_>" << std::endl;
  o << "  <records_>" << std::endl;
  uint32_t begin = v.get_first_record();
  for (uint32_t i = begin; i < v.get_records_consumed(); ++i) {
    HashTmpBin::Record* record = v.get_record(i);
    o << "    <record id=\"" << i << "\" hash=\"" << assorted::Hex(record->hash_, 16) << "\"";
    if (record->next_) {
      o << " next_rec=\"" << record->next_ << "\"";
    }
    o << ">";
    o << "<key>" << assorted::HexString(std::string(record->get_key(), record->key_length_))
      << "</key>";
    o << "<payload>"
      << assorted::HexString(std::string(record->get_payload(), record->payload_length_))
      << "</payload>";
    o << record->xct_id_;
    o << "</record>" << std::endl;
  }
  o << "  </records_>" << std::endl;
  o << "</HashTmpBin>";
  return o;
}


}  // namespace hash
}  // namespace storage
}  // namespace foedus
