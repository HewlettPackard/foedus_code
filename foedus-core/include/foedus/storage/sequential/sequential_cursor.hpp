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
     * \b Not \b implemented \b yet.
     */
    kEpochFirstMode,
  };

  /**
  * @brief A cursor interface to read tuples from a sequential storage.
  * @details
  * Unlike other storages, the only read-access to sequential storages is,
  * as the name implies, a full sequential scan. This cursor interface is
  * thus optimized for cases where we return millions of tuples.
  */
  struct NodeState {
  };

  SequentialCursor(
    thread::Thread* context,
    sequential::SequentialStorage storage);

  thread::Thread*               get_context() { return context_;}
  sequential::SequentialStorage get_storage() { return storage_; }

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
   * @invariant from_epoch_.is_valid() : To specify no-limit, use system-initial epoch.
   */
  Epoch                         from_epoch_;
  /**
   * Exclusive end of epochs to read.
   * @invariant to_epoch_.is_valid() : To specify no-limit, use system's current epoch.one_more().
   */
  Epoch                         to_epoch_;

  /** Number of tuples read so far. Just a statistics. */
  uint64_t                      tuples_so_far_;

  /** How far we have read from each node. Index is node ID. */
  NodeState*                    states_;
};


}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_CURSOR_HPP_
