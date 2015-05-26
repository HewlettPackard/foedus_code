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
#include "foedus/storage/sequential/sequential_cursor.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace sequential {

SequentialCursor::SequentialCursor(
  thread::Thread* context,
  const SequentialStorage& storage,
  void* buffer,
  uint64_t buffer_size,
  OrderMode order_mode,
  Epoch from_epoch,
  Epoch to_epoch,
  int32_t node_filter)
  : context_(context),
    xct_(&context->get_current_xct()),
    engine_(context->get_engine()),
    storage_(storage),
    from_epoch_(
      from_epoch.is_valid() ? from_epoch : engine_->get_savepoint_manager()->get_earliest_epoch()),
    to_epoch_(
      to_epoch.is_valid() ? to_epoch : engine_->get_xct_manager()->get_current_global_epoch()),
    latest_snapshot_epoch_(engine_->get_snapshot_manager()->get_snapshot_epoch()),
    node_filter_(node_filter),
    node_count_(engine_->get_soc_count()),
    order_mode_(order_mode),
    buffer_(reinterpret_cast<SequentialRecordBatch*>(buffer)),
    buffer_size_(buffer_size),
    buffer_pages_(buffer_size / kPageSize) {
  buffer_cur_page_ = 0;
  buffer_cur_page_records_ = 0;
  buffer_cur_record_ = 0;
  current_node_ = 0;
  finished_snapshots_ = false;
  finished_safe_volatiles_ = false;
  finished_unsafe_volatiles_ = false;
  for (uint16_t i = 0; i < node_count_; ++i) {
    states_.emplace_back(i);
  }

  Epoch current_global_epoch = engine_->get_xct_manager()->get_current_global_epoch();
  ASSERT_ND(from_epoch_.is_valid());
  ASSERT_ND(to_epoch_.is_valid());
  ASSERT_ND(from_epoch_ <= to_epoch_);

  if (context_->get_current_xct().get_isolation_level() == xct::kSnapshot
    || (latest_snapshot_epoch_.is_valid() && to_epoch_ <= latest_snapshot_epoch_)) {
    snapshot_only_ = true;
    safe_epoch_only_ = true;
    finished_safe_volatiles_ = true;
    finished_unsafe_volatiles_ = true;
  } else {
    snapshot_only_ = false;
    if (to_epoch_ <= current_global_epoch) {
      safe_epoch_only_ = true;
      finished_unsafe_volatiles_ = true;
    } else {
      // only in this case, we have to take a lock
      safe_epoch_only_ = false;
    }
  }
}

SequentialCursor::~SequentialCursor() {
  states_.clear();
}

SequentialCursor::NodeState::NodeState(uint16_t node_id) : node_id_(node_id) {
  snapshot_cur_head_ = 0;
  snapshot_cur_head_read_pages_ = 0;
  volatile_cur_page_ = nullptr;
}
SequentialCursor::NodeState::~NodeState() {}

SequentialRecordIterator::SequentialRecordIterator()
  : batch_(nullptr),
    from_epoch_(INVALID_EPOCH),
    to_epoch_(INVALID_EPOCH),
    record_count_(0) {
  cur_record_ = 0;
  cur_record_length_ = 0;
  cur_offset_ = 0;
  cur_record_epoch_ = INVALID_EPOCH;
  stat_skipped_records_ = 0;
}

SequentialRecordIterator::SequentialRecordIterator(
  const SequentialRecordBatch* batch,
  Epoch from_epoch,
  Epoch to_epoch)
  : batch_(batch),
    from_epoch_(from_epoch),
    to_epoch_(to_epoch),
    record_count_(batch->get_record_count()) {
  cur_record_ = 0;
  cur_record_length_ = batch_->get_record_length(0);
  cur_offset_ = 0;
  cur_record_epoch_ = batch->get_epoch_from_offset(0);
  ASSERT_ND(cur_record_epoch_.is_valid());
  stat_skipped_records_ = 0;
  if (!in_epoch_range(cur_record_epoch_)) {
    ++stat_skipped_records_;
    next();
  }
}

ErrorCode SequentialCursor::next_batch(SequentialRecordIterator* out) {
  bool found = false;
  if (!finished_snapshots_) {
    CHECK_ERROR_CODE(next_batch_snapshot(out, &found));
    if (found) {
      return kErrorCodeOk;
    } else {
      DVLOG(1) << "Finished reading snapshot pages:" << *this;
      finished_snapshots_ = true;
    }
  }

  if (!finished_safe_volatiles_) {
    CHECK_ERROR_CODE(next_batch_safe_volatiles(out, &found));
    if (found) {
      return kErrorCodeOk;
    } else {
      DVLOG(1) << "Finished reading safe volatile pages:" << *this;
      finished_safe_volatiles_ = true;
    }
  }

  if (!finished_unsafe_volatiles_) {
    CHECK_ERROR_CODE(next_batch_unsafe_volatiles(out, &found));
    if (found) {
      return kErrorCodeOk;
    } else {
      DVLOG(1) << "Finished reading unsafe volatile pages:" << *this;
      finished_unsafe_volatiles_ = true;
    }
  }

  ASSERT_ND(!is_valid());

  return kErrorCodeOk;
}

ErrorCode SequentialCursor::next_batch_snapshot(
  SequentialRecordIterator* /*out*/,
  bool* /*found*/) {
  // TODO(Hideaki) implement
  return kErrorCodeOk;
}

ErrorCode SequentialCursor::next_batch_safe_volatiles(
  SequentialRecordIterator* /*out*/,
  bool* /*found*/) {
  // TODO(Hideaki) implement
  return kErrorCodeOk;
}

ErrorCode SequentialCursor::next_batch_unsafe_volatiles(
  SequentialRecordIterator* /*out*/,
  bool* /*found*/) {
  // TODO(Hideaki) implement
  return kErrorCodeOk;
}

std::ostream& operator<<(std::ostream& o, const SequentialCursor& /*v*/) {
  o << "<SequentialCursor>";
  // TODO(Hideaki) implement
  o << "</SequentialCursor>";
  return o;
}


}  // namespace sequential
}  // namespace storage
}  // namespace foedus
