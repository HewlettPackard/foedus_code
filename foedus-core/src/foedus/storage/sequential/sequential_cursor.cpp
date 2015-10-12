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

#include <algorithm>
#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace sequential {

Epoch max_from_epoch_snapshot_epoch(Epoch from_epoch, Epoch latest_snapshot_epoch) {
  if (!latest_snapshot_epoch.is_valid()) {
    return from_epoch;
  }
  if (from_epoch > latest_snapshot_epoch) {
    return from_epoch;
  } else {
    return latest_snapshot_epoch;
  }
}

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
    resolver_(engine_->get_memory_manager()->get_global_volatile_page_resolver()),
    storage_(storage),
    from_epoch_(
      from_epoch.is_valid() ? from_epoch : engine_->get_savepoint_manager()->get_earliest_epoch()),
    to_epoch_(
      to_epoch.is_valid() ? to_epoch : engine_->get_xct_manager()->get_current_grace_epoch()),
    latest_snapshot_epoch_(engine_->get_snapshot_manager()->get_snapshot_epoch()),
    from_epoch_volatile_(max_from_epoch_snapshot_epoch(from_epoch_, latest_snapshot_epoch_)),
    node_filter_(node_filter),
    node_count_(engine_->get_soc_count()),
    order_mode_(order_mode),
    buffer_(reinterpret_cast<SequentialRecordBatch*>(buffer)),
    buffer_size_(buffer_size),
    buffer_pages_(buffer_size / kPageSize) {
  ASSERT_ND(buffer_size >= kPageSize);
  current_node_ = 0;
  finished_snapshots_ = false;
  finished_safe_volatiles_ = false;
  finished_unsafe_volatiles_ = false;
  states_.clear();

  grace_epoch_ = engine_->get_xct_manager()->get_current_grace_epoch();
  ASSERT_ND(from_epoch_.is_valid());
  ASSERT_ND(to_epoch_.is_valid());
  ASSERT_ND(from_epoch_ <= to_epoch_);

  if (xct_->get_isolation_level() == xct::kSnapshot
    || (latest_snapshot_epoch_.is_valid() && to_epoch_ <= latest_snapshot_epoch_)) {
    snapshot_only_ = true;
    safe_epoch_only_ = true;
    finished_safe_volatiles_ = true;
    finished_unsafe_volatiles_ = true;
  } else {
    snapshot_only_ = false;
    if (to_epoch_ <= grace_epoch_) {
      safe_epoch_only_ = true;
      // We do NOT rule out reading unsafe pages yet.
      // Even a safe page might be conservatively deemed as unsafe in our logic,
      // so we must make sure we go on to unsafe-volatile phase too.
      // Real-check happens in next_batch_unsafe_volatiles().
      // finished_unsafe_volatiles_ = true;
    } else {
      // only in this case, we have to take a lock
      safe_epoch_only_ = false;
    }
  }

  if (!latest_snapshot_epoch_.is_valid() || latest_snapshot_epoch_ < from_epoch_) {
    finished_snapshots_ = true;
  }
}

SequentialCursor::~SequentialCursor() {
  states_.clear();
}

SequentialCursor::NodeState::NodeState(uint16_t node_id) : node_id_(node_id) {
  volatile_cur_core_ = 0;
  snapshot_cur_head_ = 0;
  snapshot_cur_buffer_ = 0;
  snapshot_buffered_pages_ = 0;
  snapshot_buffer_begin_ = 0;
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
  out->reset();
  if (states_.empty()) {
    CHECK_ERROR_CODE(init_states());
  }

  bool found = false;
  if (!finished_snapshots_) {
    CHECK_ERROR_CODE(next_batch_snapshot(out, &found));
    if (found) {
      return kErrorCodeOk;
    } else {
      DVLOG(1) << "Finished reading snapshot pages:";
      DVLOG(2) << *this;
      ASSERT_ND(finished_snapshots_);
      refresh_grace_epoch();
    }
  }

  if (!finished_safe_volatiles_) {
    CHECK_ERROR_CODE(next_batch_safe_volatiles(out, &found));
    if (found) {
      return kErrorCodeOk;
    } else {
      DVLOG(1) << "Finished reading safe volatile pages:";
      DVLOG(2) << *this;
      ASSERT_ND(finished_safe_volatiles_);
      refresh_grace_epoch();
    }
  }

  if (!finished_unsafe_volatiles_) {
    CHECK_ERROR_CODE(next_batch_unsafe_volatiles(out, &found));
    if (found) {
      return kErrorCodeOk;
    } else {
      DVLOG(1) << "Finished reading unsafe volatile pages:";
      DVLOG(2) << *this;
      ASSERT_ND(finished_unsafe_volatiles_);
    }
  }

  ASSERT_ND(!is_valid());
  return kErrorCodeOk;
}

ErrorCode SequentialCursor::init_states() {
  DVLOG(0) << "Initializing states...";
  DVLOG(1) << *this;
  for (uint16_t node_id = 0; node_id < node_count_; ++node_id) {
    states_.emplace_back(node_id);
  }

  // Ignore records that are before the truncate epoch.
  // This is trivially done by overwriting from_epoch with truncate epoch.
  CHECK_ERROR_CODE(storage_.optimistic_read_truncate_epoch(context_, &truncate_epoch_));
  ASSERT_ND(truncate_epoch_.is_valid());
  DVLOG(0) << "truncate_epoch_=" << truncate_epoch_;
  if (truncate_epoch_ > from_epoch_) {
    LOG(INFO) << "Overwrote from_epoch (" << from_epoch_ << ") with"
      << " truncate_epoch(" << truncate_epoch_ << ")";
    from_epoch_ = truncate_epoch_;
  }

  // initialize snapshot page status
  if (!finished_snapshots_) {
    ASSERT_ND(latest_snapshot_epoch_.is_valid());
    SnapshotPagePointer root_snapshot_page_id = storage_.get_metadata()->root_snapshot_page_id_;

    // read all entries from all root pages
    uint64_t too_old_pointers = 0;
    uint64_t too_new_pointers = 0;
    uint64_t node_filtered_pointers = 0;
    uint64_t added_pointers = 0;
    uint32_t page_count = 0;
    for (SnapshotPagePointer next_page_id = root_snapshot_page_id; next_page_id != 0;) {
      ASSERT_ND(next_page_id != 0);
      ++page_count;
      SequentialRootPage* page;
      CHECK_ERROR_CODE(context_->find_or_read_a_snapshot_page(
        next_page_id,
        reinterpret_cast<Page**>(&page)));
      for (uint16_t i = 0; i < page->get_pointer_count(); ++i) {
        const HeadPagePointer& pointer = page->get_pointers()[i];
        ASSERT_ND(pointer.from_epoch_.is_valid());
        ASSERT_ND(pointer.to_epoch_.is_valid());
        uint16_t numa_node = extract_numa_node_from_snapshot_pointer(pointer.page_id_);
        if (pointer.from_epoch_ >= to_epoch_) {
          ++too_new_pointers;
          continue;
        } else if (pointer.to_epoch_ <= from_epoch_) {
          ++too_old_pointers;
          continue;
        } else if (node_filter_ >= 0 && numa_node != static_cast<uint32_t>(node_filter_)) {
          ++node_filtered_pointers;
          continue;
        } else {
          ++added_pointers;
          uint16_t node_id = extract_numa_node_from_snapshot_pointer(pointer.page_id_);
          ASSERT_ND(node_id < node_count_);
          states_[node_id].snapshot_heads_.push_back(pointer);
        }
      }
      next_page_id = page->get_next_page();
    }

    DVLOG(0) << "Read " << page_count << " root snapshot pages. added_pointers=" << added_pointers
      << ", too_old_pointers=" << too_old_pointers << ", too_new_pointers=" << too_new_pointers
      << ", node_filtered_pointers=" << node_filtered_pointers;
    if (added_pointers == 0) {
      finished_snapshots_ = true;
    }
  }

  // initialize volatile page status
  if (finished_safe_volatiles_ && finished_unsafe_volatiles_) {
    ASSERT_ND(safe_epoch_only_);
  } else {
    SequentialStoragePimpl pimpl(engine_, storage_.get_control_block());
    uint16_t thread_per_node = engine_->get_options().thread_.thread_count_per_group_;

    uint64_t empty_threads = 0;
    for (uint16_t node_id = 0; node_id < node_count_; ++node_id) {
      if (node_filter_ >= 0 && node_id != static_cast<uint32_t>(node_filter_)) {
        continue;
      }
      NodeState& state = states_[node_id];
      for (uint16_t thread_ordinal = 0; thread_ordinal < thread_per_node; ++thread_ordinal) {
        thread::ThreadId thread_id = thread::compose_thread_id(node_id, thread_ordinal);
        memory::PagePoolOffset offset = *pimpl.get_head_pointer(thread_id);
        if (offset == 0) {
          // TASK(Hideaki) This should install the head pointer to not overlook concurrent
          // insertions. Currently we will miss records in such a case. We need
          // a lock for head-installation. Overhead is not an issue because this rarely happens,
          // but we need to implement. later later.
          ++empty_threads;
          state.volatile_cur_pages_.push_back(nullptr);
          continue;
        }
        VolatilePagePointer pointer = combine_volatile_page_pointer(node_id, 0, 0, offset);
        SequentialPage* page = resolve_volatile(pointer);
        if (page->get_record_count() > 0 && page->get_first_record_epoch() >= to_epoch_) {
          // even the first record has too-new epoch, no chance. safe to ignore this thread.
          ++empty_threads;
          state.volatile_cur_pages_.push_back(nullptr);
          continue;
        }

        // from_epoch_ doesn't matter. the thread might be inserting a new record right now.
        state.volatile_cur_pages_.push_back(page);
      }
      ASSERT_ND(state.volatile_cur_pages_.size() == thread_per_node);
    }
    DVLOG(0) << "Initialized volatile head pages. empty_threads=" << empty_threads;
  }

  DVLOG(0) << "Initialized states.";
  DVLOG(1) << *this;
  return kErrorCodeOk;
}

ErrorCode SequentialCursor::next_batch_snapshot(
  SequentialRecordIterator* out,
  bool* found) {
  ASSERT_ND(!finished_snapshots_);
  ASSERT_ND(order_mode_ == kNodeFirstMode);  // TASK(Hideaki) implement other modes
  // When we implement epoch_first mode, remember that we have to split the buffer to nodes.
  // In the worst case we have to read one-page at a time...
  // The code below assumed node-first mode, so we can fully use the buffer for each node.
  while (current_node_ < node_count_) {
    NodeState& state = states_[current_node_];
    if (state.snapshot_cur_buffer_ >= state.snapshot_buffered_pages_) {
      // need to buffer more
      CHECK_ERROR_CODE(buffer_snapshot_pages(current_node_));
      if (state.snapshot_cur_buffer_ >= state.snapshot_buffered_pages_) {
        ++current_node_;
        continue;
      }
    }

    // okay, we have a page to return
    ASSERT_ND(state.snapshot_cur_buffer_ < state.snapshot_buffered_pages_);
    *out = SequentialRecordIterator(buffer_ + state.snapshot_cur_buffer_, from_epoch_, to_epoch_);
    *found = true;
    ++state.snapshot_cur_buffer_;
    return kErrorCodeOk;
  }

  ASSERT_ND(*found == false);
  finished_snapshots_ = true;
  current_node_ = 0;
  DVLOG(0) << "Finished reading snapshot pages: ";
  DVLOG(1) << *this;
  return kErrorCodeOk;
}

ErrorCode SequentialCursor::buffer_snapshot_pages(uint16_t node) {
  NodeState& state = states_[current_node_];
  if (state.snapshot_cur_head_ == state.snapshot_heads_.size()) {
    DVLOG(1) << "Node-" << node << " doesn't have any more snapshot pages:";
    DVLOG(2) << *this;
    return kErrorCodeOk;
  }

  // do we have to switch to next linked-list?
  while (state.snapshot_buffer_begin_ + state.snapshot_cur_buffer_
      >= state.get_cur_head().page_count_) {
    DVLOG(1) << "Completed node-" << node << "'s head-"
      << state.snapshot_cur_head_ << ": ";
    DVLOG(2) << *this;
    ++state.snapshot_cur_head_;
    state.snapshot_cur_buffer_ = 0;
    state.snapshot_buffer_begin_ = 0;
    state.snapshot_buffered_pages_ = 0;
    if (state.snapshot_cur_head_ == state.snapshot_heads_.size()) {
      DVLOG(1) << "Completed node-" << node << "'s all heads: ";
      DVLOG(2) << *this;
      return kErrorCodeOk;
    }
  }

  const HeadPagePointer& head = state.get_cur_head();
  ASSERT_ND(state.snapshot_cur_buffer_ == state.snapshot_buffered_pages_);
  ASSERT_ND(state.snapshot_buffer_begin_ + state.snapshot_cur_buffer_ < head.page_count_);
  uint32_t remaining = head.page_count_ - state.snapshot_buffer_begin_ - state.snapshot_cur_buffer_;
  uint32_t to_read = std::min<uint32_t>(buffer_pages_, remaining);
  ASSERT_ND(to_read > 0);
  DVLOG(1) << "Buffering " << to_read << " pages. ";
  DVLOG(2) << *this;

  // here, we read contiguous to_read pages in one shot.
  // this means that we always bypass snapshot-cache, but shouldn't be
  // an issue considering that we are probably reading millions of pages.
  uint32_t new_begin = state.snapshot_buffer_begin_ + state.snapshot_cur_buffer_;
  SnapshotPagePointer page_id_begin = head.page_id_ + new_begin;
  CHECK_ERROR_CODE(
    context_->read_snapshot_pages(page_id_begin, to_read, reinterpret_cast<Page*>(buffer_)));
  state.snapshot_buffer_begin_ = new_begin;
  state.snapshot_cur_buffer_ = 0;
  state.snapshot_buffered_pages_ = to_read;

#ifndef NDEBUG
  // sanity checks
  for (uint32_t i = 0; i < to_read; ++i) {
    const SequentialRecordBatch* p = buffer_ + i;
    ASSERT_ND(p->header_.page_id_ == page_id_begin + i);
    ASSERT_ND(p->header_.snapshot_);
    ASSERT_ND(p->header_.get_page_type() == kSequentialPageType);
    ASSERT_ND(p->next_page_.volatile_pointer_.is_null());
    // Q: "Why +1?". A: For ex., think about the case where page_count_ == 1.
    if (i + state.snapshot_buffer_begin_ + 1U == head.page_count_) {
      ASSERT_ND(p->next_page_.snapshot_pointer_ == 0);
    } else {
      ASSERT_ND(p->next_page_.snapshot_pointer_ == page_id_begin + i + 1U);
    }
  }
#endif  // NDEBUG
  return kErrorCodeOk;
}

SequentialCursor::VolatileCheckPageResult SequentialCursor::next_batch_safe_volatiles_check_page(
  const SequentialPage* page) const {
  if (page == nullptr) {
    DVLOG(1) << "Skipped empty core. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ".: ";
    DVLOG(2) << *this;
    return kNextCore;
  }
  // Even if we have a volatile page, is it safe to read from?
  // If not, we skip reading here and will resume in the unsafe part.
  VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
  if (next_pointer.is_null()) {
    // Tail page is always unsafe. We don't know when next-pointer will be installed.
    DVLOG(1) << "Skipped tail page. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ".: ";
    DVLOG(2) << *this;
    return kNextCore;
  }

  if (page->get_record_count() == 0) {
    // if it's not the tail, even an empty page (which should be rare) is safe.
    // we just move on to next page
    LOG(INFO) << "Interesting. Empty non-tail page. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ". page= " << page->header();
    return kNextPage;
  }

  // All records in this page have this epoch.
  Epoch epoch = page->get_first_record_epoch();
  if (epoch >= to_epoch_) {
    DVLOG(1) << "Reached to_epoch. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ".: ";
    DVLOG(2) << *this;
    return kNextCore;
  } else if (epoch >= grace_epoch_) {
    DVLOG(1) << "Reached unsafe page. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ".: ";
    DVLOG(2) << *this;
    return kNextCore;
  } else if (latest_snapshot_epoch_.is_valid() && epoch <= latest_snapshot_epoch_) {
    LOG(INFO) << "Interesting. Records in this volatile page are already snapshotted,"
      << " but this page is not dropped yet. This can happen during snapshotting."
      << " node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ". page= " << page->header();
    return kNextPage;
  }

  // okay, this page is safe to read!
  if (epoch < from_epoch_) {
    DVLOG(2) << "Skipping too-old epoch. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ".: ";
    DVLOG(3) << *this;
    return kNextPage;
  }

  // okay, safe and not too old.
  return kValidPage;
}

ErrorCode SequentialCursor::next_batch_safe_volatiles(
  SequentialRecordIterator* out,
  bool* found) {
  ASSERT_ND(!finished_safe_volatiles_);
  ASSERT_ND(order_mode_ == kNodeFirstMode);  // TASK(Hideaki) implement other modes
  while (current_node_ < node_count_) {
    if (node_filter_ >= 0 && current_node_ != static_cast<uint32_t>(node_filter_)) {
      ++current_node_;
      continue;
    }
    NodeState& state = states_[current_node_];
    while (state.volatile_cur_core_ < state.volatile_cur_pages_.size()) {
      SequentialPage* page = state.volatile_cur_pages_[state.volatile_cur_core_];
      VolatileCheckPageResult check_result = next_batch_safe_volatiles_check_page(page);
      if (check_result == kNextCore) {
        ++state.volatile_cur_core_;
      } else {
        ASSERT_ND(check_result == kValidPage || check_result == kNextPage);
        VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
        SequentialPage* next_page = resolve_volatile(next_pointer);
        state.volatile_cur_pages_[state.volatile_cur_core_] = next_page;
        if (check_result == kValidPage) {
          *out = SequentialRecordIterator(
            reinterpret_cast<SequentialRecordBatch*>(page),
            from_epoch_volatile_,
            to_epoch_);
          *found = true;
          return kErrorCodeOk;
        }
      }
    }

    DVLOG(0) << "Finished reading all safe epochs in node-" << current_node_ << ": ";
    DVLOG(1) << *this;
    ++current_node_;
  }

  ASSERT_ND(*found == false);
  finished_safe_volatiles_ = true;
  for (uint16_t node = 0; node < node_count_; ++node) {
    states_[node].volatile_cur_core_ = 0;
  }
  current_node_ = 0;
  DVLOG(0) << "Finished reading safe volatile pages: ";
  DVLOG(1) << *this;
  return kErrorCodeOk;
}

SequentialCursor::VolatileCheckPageResult SequentialCursor::next_batch_unsafe_volatiles_check_page(
  const SequentialPage* page) const {
  if (page == nullptr) {
    DVLOG(1) << "Skipped empty core. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ".: ";
    DVLOG(2) << *this;
    return kNextCore;
  }

  if (UNLIKELY(page->get_record_count() == 0)) {
    // This is most likely a tail page. Just make sure it's safe
    // by taking page-version set if that is the case.
    return kValidPage;
  }

  ASSERT_ND(page->get_record_count() > 0);
  Epoch epoch = page->get_first_record_epoch();
  if (epoch >= to_epoch_) {
    DVLOG(1) << "Reached to_epoch. node=" << current_node_ << ", core="
      << states_[current_node_].volatile_cur_core_ << ".: ";
    DVLOG(2) << *this;
    return kNextCore;
  }

  return kValidPage;
}

ErrorCode SequentialCursor::next_batch_unsafe_volatiles(
  SequentialRecordIterator* out,
  bool* found) {
  ASSERT_ND(!finished_unsafe_volatiles_);
  // mode doesn't matter when we are reading unsafe epochs. we just read them all one by one.

  // if the record is in current global epoch, we have to take it as read-set for serializability.
  // records in grace epoch are fine. This transaction will be surely in the current global epoch
  // or later, so the dependency is trivially met.
  bool serializable = xct_->get_isolation_level() == xct::kSerializable;
  while (current_node_ < node_count_) {
    if (node_filter_ >= 0 && current_node_ != static_cast<uint32_t>(node_filter_)) {
      ++current_node_;
      continue;
    }
    NodeState& state = states_[current_node_];
    while (state.volatile_cur_core_ < state.volatile_cur_pages_.size()) {
      SequentialPage* page = state.volatile_cur_pages_[state.volatile_cur_core_];
      VolatileCheckPageResult check_result = next_batch_unsafe_volatiles_check_page(page);
      if (check_result == kNextCore) {
        ++state.volatile_cur_core_;
      } else {
        ASSERT_ND(check_result == kValidPage);
        // In unsafe page, we need to be careful to tell kValidPage/kNextPage apart.
        // So, do it here.

        VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
        bool tail_page = next_pointer.is_null();
        uint16_t record_count = page->get_record_count();
        if (serializable) {
          // let's protect the above two information with page version
          assorted::memory_fence_consume();
          PageVersionStatus observed = page->header().page_version_.status_;
          assorted::memory_fence_consume();
          if ((tail_page && !page->next_page().volatile_pointer_.is_null())
              || page->get_record_count() != record_count) {
            LOG(INFO) << "Wow, super rare. just installed next page or added a new record!";
            continue;  // retry. concurrent thread has now installed it!
          }

          // If not tail page, this page is already safe.
          // Otherwise, we have to protect the fact that this was a tail page by
          // taking a page-version set if the transaction is serializable.
          if (tail_page) {
            CHECK_ERROR_CODE(xct_->add_to_page_version_set(
              &page->header().page_version_,
              observed));
          }
        }

        // because of the way each thread appends, the last record in this page
        // always has the largest in-epoch ordinal. so, we just need it for read-set
        if (serializable && record_count > 0) {
          Epoch epoch = page->get_first_record_epoch();
          if (epoch > grace_epoch_) {
            uint16_t offset = page->get_record_offset(record_count - 1);
            xct::RwLockableXctId* owner_id = page->owner_id_from_offset(offset);
            CHECK_ERROR_CODE(xct_->add_to_read_set(
              context_,
              storage_.get_id(),
              owner_id->xct_id_,
              owner_id));
          }
        }

        if (next_pointer.is_null()) {
          state.volatile_cur_pages_[state.volatile_cur_core_] = nullptr;
        } else {
          SequentialPage* next_page = resolve_volatile(next_pointer);
          state.volatile_cur_pages_[state.volatile_cur_core_] = next_page;
        }

        *out = SequentialRecordIterator(
          reinterpret_cast<SequentialRecordBatch*>(page),
          from_epoch_volatile_,
          to_epoch_);
        *found = true;
        return kErrorCodeOk;
      }
    }

    DVLOG(0) << "Finished reading all unsafe epochs in node-" << current_node_ << ": ";
    DVLOG(1) << *this;
    ++current_node_;
  }

  ASSERT_ND(*found == false);
  finished_unsafe_volatiles_ = true;

  return kErrorCodeOk;
}

SequentialPage* SequentialCursor::resolve_volatile(VolatilePagePointer pointer) const {
  return reinterpret_cast<SequentialPage*>(resolver_.resolve_offset(pointer));
}

void SequentialCursor::refresh_grace_epoch() {
  Epoch new_grace_epoch = engine_->get_xct_manager()->get_current_grace_epoch();
  ASSERT_ND(new_grace_epoch >= grace_epoch_);
  grace_epoch_ = new_grace_epoch;
}

std::ostream& operator<<(std::ostream& o, const SequentialCursor& v) {
  o << "<SequentialCursor>" << std::endl;
  o << "  " << v.get_storage() << std::endl;
  o << "  <from_epoch>" << v.get_from_epoch() << "</from_epoch>" << std::endl;
  o << "  <to_epoch>" << v.get_to_epoch() << "</to_epoch>" << std::endl;
  o << "  <order_mode>" << v.order_mode_ << "</order_mode>" << std::endl;
  o << "  <node_filter>" << v.node_filter_ << "</node_filter>" << std::endl;
  o << "  <snapshot_only_>" << v.snapshot_only_ << "</snapshot_only_>" << std::endl;
  o << "  <safe_epoch_only_>" << v.safe_epoch_only_ << "</safe_epoch_only_>" << std::endl;
  o << "  <buffer_>" << v.buffer_ << "</buffer_>" << std::endl;
  o << "  <buffer_size>" << v.buffer_size_ << "</buffer_size>" << std::endl;
  o << "  <buffer_pages_>" << v.buffer_pages_ << "</buffer_pages_>" << std::endl;
  o << "  <current_node_>" << v.current_node_ << "</current_node_>" << std::endl;
  o << "  <finished_snapshots_>" << v.finished_snapshots_ << "</finished_snapshots_>" << std::endl;
  o << "  <finished_safe_volatiles_>" << v.finished_safe_volatiles_
    << "</finished_safe_volatiles_>" << std::endl;
  o << "  <finished_unsafe_volatiles_>" << v.finished_unsafe_volatiles_
    << "</finished_unsafe_volatiles_>" << std::endl;
  o << "</SequentialCursor>";
  return o;
}


}  // namespace sequential
}  // namespace storage
}  // namespace foedus
