/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_page_impl.hpp"

#include "foedus/assorted/raw_atomics.hpp"

namespace foedus {
namespace storage {
namespace sequential {
bool SequentialPage::append_record(
  xct::XctId owner_id,
  uint16_t payload_length,
  const void* payload) {
  uint16_t record_length = assorted::align8(payload_length) + kRecordOverhead;
  while (true) {
    uint64_t  cur_status = peek_status();
    Epoch     cur_earliest_epoch(static_cast<uint32_t>(cur_status >> 32));  // 0-32 bits
    bool      cur_closed = (cur_status & (1U << 31)) != 0;  // 32-33 bit
    uint16_t  cur_record_count = (cur_status >> 16) & 0x7FFFU;  // 33-48 bits
    uint16_t  cur_used_bytes = static_cast<uint16_t>(cur_status);  // 48-64 bits
    if (cur_closed ||
      cur_record_count >= kMaxSlots ||
      cur_used_bytes + record_length + sizeof(PayloadLength) * (cur_record_count + 1) > kDataSize) {
      // this page seems full. give up
      return false;
    }

    // volatile pages maintain the earliest epoch in the page
    ASSERT_ND(owner_id.get_epoch().is_valid());
    if (cur_earliest_epoch.is_valid()) {
      cur_earliest_epoch.store_min(owner_id.get_epoch());
    } else {
      cur_earliest_epoch = owner_id.get_epoch();
    }

    uint64_t new_status =
      static_cast<uint64_t>(cur_earliest_epoch.value()) << 32 |
      static_cast<uint64_t>(cur_record_count + 1) << 16 |
      static_cast<uint64_t>(cur_used_bytes + record_length);

    if (assorted::raw_atomic_compare_exchange_weak<uint64_t>(&status_, &cur_status, new_status)) {
      // CAS succeeded. put the record
      set_payload_length(cur_record_count, payload_length);
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
    bool cur_closed = (cur_status & (1U << 31)) != 0;  // 32-33 bit
    if (cur_closed) {
      return false;  // someone already closed it!
    }

    uint64_t new_status = cur_status | (1ULL << 31);
    if (assorted::raw_atomic_compare_exchange_weak<uint64_t>(&status_, &cur_status, new_status)) {
      return true;
    }
  }
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
