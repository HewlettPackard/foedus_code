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
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_CURSOR_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_CURSOR_HPP_

#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <iosfwd>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/**
 * @brief A cursor interface to read tuples from a sequential storage.
 * @ingroup SEQUENTIAL
 * @details
 * Unlike other storages, the only read-access to sequential storages is,
 * as the name implies, a full sequential scan. This cursor interface is
 * thus optimized for cases where we scan millions of records.
 * This implies that, unlike masstree's cursor, we don't have to worry about
 * infrequent overheads, such as new/delete in initialization.
 *
 * @par Example first
 * Use it as follows.
 * @code{.cpp}
 * memory::AlignedMemory buffer(1 << 16, 1 << 12, kNumaAllocOnnode, 0);
 * SequentialCursor cursor(context, storage, buffer.get_block(), 1 << 16);
 * SequentialRecordIterator it;
 * while (cursor.is_valid()) {
 *   CHECK_ERROR(cursor.next_batch(&it));
 *   while (it.is_valid()) {
 *     std::cout << std::string(it.get_cur_record_raw(), it.get_cur_record_length());
 *     ...
 *     it.next();
 *   }
 * }
 * @endcode
 *
 * @par Safe Epoch and Unsafe Epoch
 * Safe epochs are epochs before the current global epoch.
 * There will be no more transactions in such epochs. Thus, thanks to the append-only
 * nature of sequential storage, reading records in safe epochs does not need
 * any concurrency control. Unsafe epochs, OTOH, are the currrent global epoch or
 * future epochs. This cursor might do expensive synchronization if the user
 * requests to read records from unsafe epochs.
 */
class SequentialCursor {
 public:
  /** The order this cursor returns tuples. */
  enum OrderMode {
    /**
     * Returns as many records as possible from node-0, do the same from node-1,...
     * Note that even this mode might return \e unsafe epoch at last
     * because we delay reading unsafe epochs as much as possible.
     */
    kNodeFirstMode,
    /**
     * Returns records \b loosely ordered by epochs.
     * We don't guarantee true ordering even in this case, which is too expensive.
     * TASK(Hideaki) \b Not \b implemented \b yet.
     */
    kLooseEpochSortMode,
  };

  /**
   * @brief Constructs a cursor to read tuples from this storage.
   * @param[in] context Thread context of the transaction
   * @param[in] storage The sequential storage to read from
   * @param[in,out] buffer The buffer to read a number of tuples in a batch.
   * This buffer \b must \b be \b aligned for direct-IO.
   * @param[in] buffer_size Byte size of buffer. Must be at least 4kb.
   * @param[in] order_mode The order this cursor returns tuples
   * @param[in] from_epoch Inclusive beginning of epochs to read.
   * If not specified, all epochs.
   * @param[in] to_epoch Exclusive end of epochs to read. To read records in unsafe epochs,
   * specify a _future_ epoch, larger than the current global epoch (remember, it's exclusive end).
   * If not specified, all \e safe epochs (fast, but does not return records being added).
   * @param[in] node_filter If specified, returns records only in the given node. negative
   * for reading from all nodes. This is especially useful for parallelizing a scan on
   * a large sequential storage.
   * @details
   * Default parameter: the system-initial epoch for from_epoch and current-global epoch
   * for to_epoch (thus safe_epoch_only_). Assuming this storage is used for log/archive data,
   * this should be a quite common usecase. order_mode is defaulted to kNodeFirstMode.
   */
  SequentialCursor(
    thread::Thread* context,
    const sequential::SequentialStorage& storage,
    void* buffer,
    uint64_t buffer_size,
    OrderMode order_mode = kNodeFirstMode,
    Epoch from_epoch = INVALID_EPOCH,
    Epoch to_epoch = INVALID_EPOCH,
    int32_t node_filter = -1);

  ~SequentialCursor();

  thread::Thread*                       get_context() const { return context_;}
  const sequential::SequentialStorage&  get_storage() const { return storage_; }

  /** @return Inclusive beginning of epochs to read. */
  Epoch     get_from_epoch() const { return from_epoch_; }
  /** @return Exclusive end of epochs to read. */
  Epoch     get_to_epoch() const { return to_epoch_; }

  /**
   * @brief Returns a batch of records as an iterator.
   * @param[out] out an iterator over returned records.
   * @details
   * It \e might return an empty batch even when this cursor has more records to return.
   * Invoke is_valid() to check it. This method does nothing if is_valid() is already false.
   * Each batch is guaranteed to be from one node, and actually from one page.
   */
  ErrorCode next_batch(SequentialRecordIterator* out);

  bool      is_valid() const {
    return !(finished_snapshots_ && finished_safe_volatiles_ && finished_unsafe_volatiles_);
  }

  friend std::ostream& operator<<(std::ostream& o, const SequentialCursor& v);

 private:
  /**
   * @brief Represents the progress of this cursor on each SOC node.
   * @details
   * For each next_batch() call, we resume reading based on these information.
   */
  struct NodeState {
    explicit NodeState(uint16_t node_id);
    ~NodeState();
    const uint16_t                node_id_;
    uint32_t                      snapshot_cur_head_;
    uint64_t                      snapshot_cur_head_read_pages_;
    std::vector<HeadPagePointer>  snapshot_heads_;

    /**
     * When we are currently reading from volatile list, the page we are currently in.
     * Null otherwise.
     */
    SequentialPage*               volatile_cur_page_;
  };

  /// subroutines of next_batch().
  ErrorCode next_batch_snapshot(SequentialRecordIterator* out, bool* found);
  ErrorCode next_batch_safe_volatiles(SequentialRecordIterator* out, bool* found);
  ErrorCode next_batch_unsafe_volatiles(SequentialRecordIterator* out, bool* found);

  thread::Thread* const               context_;
  xct::Xct* const                     xct_;
  Engine* const                       engine_;
  sequential::SequentialStorage const storage_;
  /**
   * Inclusive beginning of epochs to read.
   * @invariant !from_epoch_.is_valid()
   */
  const Epoch                   from_epoch_;
  /**
   * Exclusive end of epochs to read.
   * @invariant !to_epoch_.is_valid()
   */
  const Epoch                   to_epoch_;

  const Epoch                   latest_snapshot_epoch_;

  const int32_t                 node_filter_;
  const uint16_t                node_count_;
  const OrderMode               order_mode_;
  /**
   * True when either the isolation level is SI, or to_epoch_ is up to the previous snapshot epoch.
   * When this is true, we just read snapshot pages without any concern on concurrency control.
   */
  bool                          snapshot_only_;
  /**
   * True when snapshot_only_ or to_epoch_ is up to the previous system epoch, meaning the cursor
   * never reads tuples in the current epoch or later without any concern on concurrency control.
   */
  bool                          safe_epoch_only_;

  SequentialRecordBatch* const  buffer_;
  const uint64_t                buffer_size_;
  /** buffer_pages_ = buffer_size_ / kPageSize */
  const uint32_t                buffer_pages_;

  /// Everything above is const. Some of them doesn't have const qual due to init() method.
  /// Everything below is mutable. in other words, they are the state of this cursor.

  /**
   * Index of the page in buffer_ we are now reading from.
   * @invariant buffer_cur_page_ <= buffer_pages_
   * (buffer_cur_page_ == buffer_pages_ means we need to read next batch)
   */
  uint32_t                      buffer_cur_page_;

  /** Number of records in the current page */
  uint16_t                      buffer_cur_page_records_;
  /**
   * Index of the record in the current page we are now reading.
   * @invariant buffer_cur_record_ <= buffer_cur_page_records_
   * (buffer_cur_record_ == buffer_cur_page_records_ means we need to read next page)
   */
  uint16_t                      buffer_cur_record_;

  uint16_t                      current_node_;

  /** whether this cursor has read all snapshot pages it should read. */
  bool                          finished_snapshots_;
  /** whether this cursor has read all volatile pages in safe epochs it should read. */
  bool                          finished_safe_volatiles_;
  /** whether this cursor has read all volatile pages in unsafe epochs it should read. */
  bool                          finished_unsafe_volatiles_;

  /** How far we have read from each node. Index is node ID. */
  std::vector<NodeState>        states_;
};


/**
 * @brief A chunk of records returned by SequentialCursor.
 * @ingroup SEQUENTIAL
 * @details
 * SequentialCursor tends to return a huge number of records.
 * For better performance, we return a page-full of records at a time.
 *
 * For now, as an optimization, this struct has the exact same format as SequentialPage,
 * which is an internal implementation detail. On the other hand, this struct is an API.
 * We keep them separate as the internal page representation might change in future.
 */
struct SequentialRecordBatch CXX11_FINAL {
  PageHeader            header_;            // +32 -> 32

  uint16_t              record_count_;      // +2 -> 34
  uint16_t              used_data_bytes_;   // +2 -> 36
  uint32_t              filler_;            // +4 -> 40

  /**
   * Pointer to next page.
   * Once it is set, the pointer and the pointed page will never be changed.
   */
  DualPagePointer       next_page_;         // +16 -> 56

  /**
   * Dynamic data part in this page, which consist of 1) record part growing forward,
   * 2) unused part, and 3) payload lengthes part growing backward.
   */
  char                  data_[kDataSize];


  uint16_t       get_record_count() const { return record_count_; }
  uint16_t       get_record_length(uint16_t index) const {
    ASSERT_ND(index < record_count_);
    return reinterpret_cast<const uint16_t*>(data_ + sizeof(data_))[-index - 1];
  }
  const xct::LockableXctId* get_xctid_from_offset(uint16_t offset) const {
    ASSERT_ND(offset + record_count_ * sizeof(uint16_t) <= kDataSize);
    return reinterpret_cast<const xct::LockableXctId*>(data_ + offset);
  }
  const char*    get_payload_from_offset(uint16_t offset) const {
    ASSERT_ND(offset + record_count_ * sizeof(uint16_t) <= kDataSize);
    return data_ + offset + sizeof(xct::LockableXctId);
  }
  Epoch          get_epoch_from_offset(uint16_t offset) const {
    return get_xctid_from_offset(offset)->xct_id_.get_epoch();
  }
};

/**
 * @brief Iterator for one SequentialRecordBatch, or a page.
 * @ingroup SEQUENTIAL
 * @details
 * This class inlines per-record methods, but not per-page methods (eg initialization).
 */
class SequentialRecordIterator CXX11_FINAL {
 public:
  SequentialRecordIterator();
  SequentialRecordIterator(const SequentialRecordBatch* batch, Epoch from_epoch, Epoch to_epoch);

  bool        is_valid() const ALWAYS_INLINE { return cur_record_ < record_count_; }
  void        next() ALWAYS_INLINE {
    while (true) {
      if (UNLIKELY(cur_record_ + 1U >= record_count_)) {
        cur_record_ = record_count_;
        break;
      }

      ++cur_record_;
      cur_offset_ += assorted::align8(cur_record_length_) + sizeof(xct::LockableXctId);
      cur_record_length_ = batch_->get_record_length(cur_record_);
      cur_record_epoch_ = batch_->get_epoch_from_offset(cur_offset_);
      ASSERT_ND(cur_record_epoch_.is_valid());
      if (LIKELY(in_epoch_range(cur_record_epoch_))) {
        break;
      }
      // we have to skip this record
      ++stat_skipped_records_;
    }
  }
  uint16_t    get_cur_record_length() const ALWAYS_INLINE {
    ASSERT_ND(is_valid());
    return cur_record_length_;
  }
  Epoch       get_cur_record_epoch() const ALWAYS_INLINE {
    ASSERT_ND(is_valid());
    return cur_record_epoch_;
  }
  /**
   * Copies the current record to the given buffer.
   * This is safe even after the cursor moves on, but it incurs one memcpy.
   * @see get_cur_record_raw()
   */
  void        copy_cur_record(char* out, uint16_t out_size) const ALWAYS_INLINE {
    ASSERT_ND(is_valid());
    const char* raw = get_cur_record_raw();
    uint16_t copy_size = std::min<uint16_t>(out_size, cur_record_length_);
    std::memcpy(out, raw, copy_size);
  }
  /**
   * Directly returns a pointer to the current record. No memcpy, but note that the pointed address
   * might become invalid once the cursor moves on. Use it with caution, like std::string::data().
   * @see copy_cur_record()
   */
  const char* get_cur_record_raw() const ALWAYS_INLINE {
    ASSERT_ND(is_valid());
    return batch_->get_payload_from_offset(cur_offset_);
  }
  bool        in_epoch_range(Epoch epoch) const ALWAYS_INLINE {
    return epoch >= from_epoch_ && epoch < to_epoch_;
  }

  /** @returns number of records we skipped so far due to from/to epoch */
  uint16_t    get_stat_skipped_records() const { return stat_skipped_records_; }
  /** @returns total number of records in this batch */
  uint16_t    get_record_count() const { return record_count_; }

 private:
  const SequentialRecordBatch* batch_;  // +8 -> 8
  Epoch     from_epoch_;                // +4 -> 12
  Epoch     to_epoch_;                  // +4 -> 16
  Epoch     cur_record_epoch_;          // +4 -> 20
  uint16_t  record_count_;              // +2 -> 22
  uint16_t  cur_record_;                // +2 -> 24
  uint16_t  cur_record_length_;         // +2 -> 26
  uint16_t  cur_offset_;                // +2 -> 28
  uint16_t  stat_skipped_records_;      // +2 -> 30
  uint16_t  filler_;                    // +2 -> 32
};

STATIC_SIZE_CHECK(sizeof(SequentialRecordBatch), kPageSize)

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_CURSOR_HPP_
