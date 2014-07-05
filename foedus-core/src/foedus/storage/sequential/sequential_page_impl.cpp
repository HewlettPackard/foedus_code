/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_page_impl.hpp"

namespace foedus {
namespace storage {
namespace sequential {
bool SequentialPage::append_record(
  xct::XctId owner_id,
  uint16_t payload_length,
  const void* payload) {
  uint16_t record_length = assorted::align8(payload_length) + kRecordOverhead;
  while (true) {
    uint64_t cur_status = peek_status();
    uint16_t cur_record_count = static_cast<uint16_t>(cur_status >> 16);  // 32-48 bits
    uint16_t cur_used_bytes = static_cast<uint16_t>(cur_status);  // 48-64 bits
    bool cur_closed = (cur_status >> 32) > 0;
    if (cur_closed ||
      cur_record_count >= kMaxSlots ||
      cur_used_bytes + record_length > kDataSize) {
      // this page seems full. give up
      return false;
    }

    uint64_t new_status =
      static_cast<uint64_t>(cur_record_count + 1) << 16 |
      static_cast<uint64_t>(cur_used_bytes + record_length);
    if (status_.compare_exchange_weak(cur_status, new_status)) {
      // CAS succeeded. put the record
      slots_.lengthes_[cur_record_count] = payload_length;
      std::memcpy(data_ + cur_used_bytes + kRecordOverhead, payload, payload_length);
      assorted::memory_fence_release();  // finish memcpy BEFORE setting xct_id
      xct::XctId* owner_id_addr = reinterpret_cast<xct::XctId*>(data_ + cur_used_bytes);
      *owner_id_addr = owner_id;
      // note that the scanner thread has to be careful on reading records that were just
      // inserted. It must make sure xct_id is already set. it can do so atomically checking
      // status (record count and length) with non-null owner-ids.
      // As we write-once, never overwrite, it can just spin on null owner-id.
      return true;
    } else {
      // CAS failed. retry, which might result in "give up"
    }
  }
}

bool SequentialPage::try_close_page() {
  while (true) {
    uint64_t cur_status = peek_status();
    bool cur_closed = (cur_status >> 32) > 0;
    if (cur_closed) {
      return false;  // someone already closed it!
    }

    uint64_t new_status = cur_status | (1ULL << 32);
    if (status_.compare_exchange_weak(cur_status, new_status)) {
      return true;
    }
  }
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
