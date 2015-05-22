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

#include "foedus/epoch.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {

/**
 * @brief A cursor interface to read tuples from a sequential storage.
 * @ingroup SEQUENTIAL
 * @details
 * Unlike other storages, the only read-access to sequential storages is,
 * as the name implies, a full sequential scan. This cursor interface is
 * thus optimized for cases where we return millions of tuples.
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
     * Returns the earlise epoch's records, then next epoch, ...
     * TASK(Hideaki) \b Not \b implemented \b yet.
     */
    kEpochFirstMode,
  };

  /**
   * @brief Represents the progress of this cursor on each SOC node.
   * @details
   * For each next_batch() call, we resume reading based on these information.
   */
  struct NodeState {
  };

  /**
   * @brief Constructs a cursor to read tuples from this storage.
   * @param[in] context Thread context of the transaction
   * @param[in] storage The sequential storage to read from
   * @param[in,out] buffer The buffer to read a number of tuples in a batch.
   * @param[in] buffer_size Byte size of buffer. Must be at least 4kb.
   * @param[in] order_mode The order this cursor returns tuples
   * @param[in] from_epoch Inclusive beginning of epochs to read.
   * @param[in] to_epoch Exclusive end of epochs to read.
   */
  SequentialCursor(
    thread::Thread* context,
    const sequential::SequentialStorage& storage,
    void* buffer,
    uint64_t buffer_size,
    OrderMode order_mode,
    Epoch from_epoch,
    Epoch to_epoch);

  /**
   * This overload uses the system-initial epoch for from_epoch and system-current epoch -1
   * for to_epoch (thus safe_epoch_only_). Assuming this storage is used for log/archive data,
   * this should be a quite common usecase. order_mode is defaulted to kNodeFirstMode.
   */
  SequentialCursor(
    thread::Thread* context,
    const sequential::SequentialStorage& storage,
    void* buffer,
    uint64_t buffer_size);

  ~SequentialCursor();

  thread::Thread*                       get_context() const { return context_;}
  const sequential::SequentialStorage&  get_storage() const { return storage_; }

  /** @return Inclusive beginning of epochs to read. */
  Epoch     get_from_epoch() const { return from_epoch_; }
  /** @return Exclusive end of epochs to read. */
  Epoch     get_to_epoch() const { return to_epoch_; }
  /** @returns Number of tuples read so far. Just a statistics. */
  uint64_t  get_tuples_so_far() const { return tuples_so_far_; }

 private:
  thread::Thread* const               context_;
  sequential::SequentialStorage const storage_;
  /**
   * Inclusive beginning of epochs to read.
   * @invariant !from_epoch_.is_valid()
   */
  Epoch                         from_epoch_;
  /**
   * Exclusive end of epochs to read.
   * @invariant !to_epoch_.is_valid()
   */
  Epoch                         to_epoch_;

  uint16_t                      node_count_;
  OrderMode                     order_mode_;
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

  void* const                   buffer_;
  const uint64_t                buffer_size_;
  /** buffer_pages_ = buffer_size_ / 4kb */
  const uint32_t                buffer_pages_;

  // everything above is const. Some of them doesn't have const qual due to init() method.

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

  /**
   * Epoch of the record (page) we are currently reading.
   */
  Epoch                         cur_record_epoch_;

  /** Number of tuples read so far. Just a statistics. */
  uint64_t                      tuples_so_far_;

  uint16_t                      current_node_;

  /** How far we have read from each node. Index is node ID. */
  NodeState*                    states_;

  void init(OrderMode order_mode, Epoch from_epoch, Epoch to_epoch);
};


}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_CURSOR_HPP_
