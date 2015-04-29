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
#include "foedus/storage/hash/hash_composer_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/merge_sort.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_partitioner_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace hash {

inline HashDataPage* resolve_data_impl(
  const memory::GlobalVolatilePageResolver& resolver,
  VolatilePagePointer pointer) {
  if (pointer.is_null()) {
    return nullptr;
  }
  HashDataPage* ret = reinterpret_cast<HashDataPage*>(resolver.resolve_offset(pointer));
  ASSERT_ND(ret->header().get_page_type() == kHashDataPageType);
  ASSERT_ND(ret->header().page_id_ == pointer.word);
  return ret;
}

inline HashIntermediatePage* resolve_intermediate_impl(
  const memory::GlobalVolatilePageResolver& resolver,
  VolatilePagePointer pointer) {
  if (pointer.is_null()) {
    return nullptr;
  }
  HashIntermediatePage* ret
    = reinterpret_cast<HashIntermediatePage*>(resolver.resolve_offset(pointer));
  ASSERT_ND(ret->header().get_page_type() == kHashIntermediatePageType);
  ASSERT_ND(ret->header().page_id_ == pointer.word);
  return ret;
}

///////////////////////////////////////////////////////////////////////
///
///  HashComposer methods
///
///////////////////////////////////////////////////////////////////////
HashComposer::HashComposer(Composer *parent)
  : engine_(parent->get_engine()),
    storage_id_(parent->get_storage_id()),
    storage_(engine_, storage_id_),
    volatile_resolver_(engine_->get_memory_manager()->get_global_volatile_page_resolver()) {
  ASSERT_ND(storage_.exists());
}

ErrorStack HashComposer::compose(const Composer::ComposeArguments& args) {
  VLOG(0) << to_string() << " composing with " << args.log_streams_count_ << " streams.";
  debugging::StopWatch stop_watch;

  snapshot::MergeSort merge_sort(
    storage_id_,
    kHashStorage,
    args.base_epoch_,
    args.log_streams_,
    args.log_streams_count_,
    kHashMaxLevels,
    args.work_memory_);
  CHECK_ERROR(merge_sort.initialize());

  HashComposeContext context(
    engine_,
    &merge_sort,
    args.snapshot_writer_,
    args.previous_snapshot_files_,
    args.root_info_page_);
  CHECK_ERROR(context.execute());

  CHECK_ERROR(merge_sort.uninitialize());  // no need for scoped release. its destructor is safe.

  stop_watch.stop();
  LOG(INFO) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

ErrorStack HashComposer::construct_root(const Composer::ConstructRootArguments& /*args*/) {
  // Unlike array package, this method might do lots of I/O.
  // In each reducer, we only created data pages. They don't create intermediate pages
  // and instead they emit linked-lists of HashComposedBinsPage and HashRootInfoPage
  // pointing to them. We combine these information here, writing out new intermediate pages.

/* TODO(Hideaki) so much for today..
  // compose() created root_info_pages that contain pointers to fill in the root page,
  // so we just find non-zero entry and copy it to root page.
  uint8_t levels = storage_.get_levels();
  snapshot::SnapshotId new_snapshot_id = args.snapshot_writer_->get_snapshot_id();
  Epoch system_initial_epoch = engine_->get_savepoint_manager()->get_initial_durable_epoch();
  HashPage* root_page = reinterpret_cast<HashPage*>(args.snapshot_writer_->get_page_base());
  SnapshotPagePointer page_id = storage_.get_metadata()->root_snapshot_page_id_;
  SnapshotPagePointer new_page_id = args.snapshot_writer_->get_next_page_id();
  *args.new_root_page_pointer_ = new_page_id;

  uint64_t root_interval = LookupRouteFinder(levels, payload_size).get_records_in_leaf();
  for (uint8_t level = 1; level < levels; ++level) {
    root_interval *= kInteriorFanout;
  }
  HashRange range(0, root_interval, storage_.get_hash_size());
  if (page_id != 0) {
    WRAP_ERROR_CODE(args.previous_snapshot_files_->read_page(page_id, root_page));
    ASSERT_ND(root_page->header().storage_id_ == storage_id_);
    ASSERT_ND(root_page->header().page_id_ == page_id);
    ASSERT_ND(root_page->get_hash_range() == range);
    root_page->header().page_id_ = new_page_id;
  } else {
    root_page->initialize_snapshot_page(
      system_initial_epoch,
      storage_id_,
      new_page_id,
      payload_size,
      levels - 1,
      range);
  }

  uint64_t child_interval = root_interval / kInteriorFanout;
  uint16_t root_children = assorted::int_div_ceil(storage_.get_hash_size(), child_interval);

  // overwrite pointers with root_info_pages.
  for (uint32_t i = 0; i < args.root_info_pages_count_; ++i) {
    const HashRootInfoPage* casted
      = reinterpret_cast<const HashRootInfoPage*>(args.root_info_pages_[i]);
    for (uint16_t j = 0; j < root_children; ++j) {
      SnapshotPagePointer pointer = casted->pointers_[j];
      if (pointer != 0) {
        ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(pointer) == new_snapshot_id);
        DualPagePointer& record = root_page->get_interior_record(j);
        // partitioning has no overlap, so this must be the only overwriting pointer
        ASSERT_ND(record.snapshot_pointer_ == 0 ||
          extract_snapshot_id_from_snapshot_pointer(record.snapshot_pointer_)
            != new_snapshot_id);
        record.snapshot_pointer_ = pointer;
      }
    }
    for (uint16_t j = root_children; j < kInteriorFanout; ++j) {
      ASSERT_ND(casted->pointers_[j] == 0);
    }
  }

  // even in initial snapshot, all pointers must be set because we create empty pages
  // even if some sub-tree receives no logs.
  for (uint16_t j = 0; j < root_children; ++j) {
    ASSERT_ND(root_page->get_interior_record(j).snapshot_pointer_ != 0);
  }

  WRAP_ERROR_CODE(args.snapshot_writer_->dump_pages(0, 1));
  ASSERT_ND(args.snapshot_writer_->get_next_page_id() == new_page_id + 1ULL);
  // AFTER writing out the root page, install the pointer to new root page
  storage_.get_control_block()->root_page_pointer_.snapshot_pointer_ = new_page_id;
  storage_.get_control_block()->meta_.root_snapshot_page_id_ = new_page_id;
*/
  return kRetOk;
}

inline HashDataPage* HashComposer::resolve_data(VolatilePagePointer pointer) const {
  return resolve_data_impl(volatile_resolver_, pointer);
}
inline HashIntermediatePage* HashComposer::resolve_intermediate(VolatilePagePointer pointer) const {
  return resolve_intermediate_impl(volatile_resolver_, pointer);
}

std::string HashComposer::to_string() const {
  return std::string("HashComposer-") + std::to_string(storage_id_);
}

///////////////////////////////////////////////////////////////////////
///
///  HashComposeContext methods
///
///////////////////////////////////////////////////////////////////////
HashComposeContext::HashComposeContext(
  Engine*                           engine,
  snapshot::MergeSort*              merge_sort,
  snapshot::SnapshotWriter*         snapshot_writer,
  cache::SnapshotFileSet*           previous_snapshot_files,
  Page*                             root_info_page)
  : engine_(engine),
    merge_sort_(merge_sort),
    system_initial_epoch_(engine->get_savepoint_manager()->get_initial_durable_epoch()),
    storage_id_(merge_sort_->get_storage_id()),
    snapshot_id_(snapshot_writer->get_snapshot_id()),
    storage_(engine, storage_id_),
    snapshot_writer_(snapshot_writer),
    previous_snapshot_files_(previous_snapshot_files),
    root_info_page_(reinterpret_cast<HashRootInfoPage*>(root_info_page)),
    partitionable_(engine_->get_soc_count() > 1U),
    levels_(storage_.get_levels()),
    bin_bits_(storage_.get_bin_bits()),
    bin_shifts_(storage_.get_bin_shifts()),
    root_children_(storage_.get_root_children()),
    numa_node_(snapshot_writer->get_numa_node()),
    total_bin_count_(storage_.get_bin_count()),
    previous_root_page_pointer_(storage_.get_metadata()->root_snapshot_page_id_),
    volatile_resolver_(engine->get_memory_manager()->get_global_volatile_page_resolver()) {
  cur_path_memory_.alloc(
    kPageSize * kHashMaxLevels,
    kPageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  cur_path_ = reinterpret_cast<HashIntermediatePage*>(cur_path_memory_.get_block());
  cur_path_lowest_level_ = levels_;
  cur_path_valid_range_ = HashBinRange(0, 0);

  cur_bin_ = kCurBinNotOpened;
  cur_intermediate_tail_ = nullptr;

  data_page_io_memory_.alloc(
    kPageSize,
    kPageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);

  allocated_pages_ = 0;
  allocated_intermediates_ = 0;
  page_base_ = reinterpret_cast<HashDataPage*>(snapshot_writer_->get_page_base());
  max_pages_ = snapshot_writer_->get_page_size();
  intermediate_base_
    = reinterpret_cast<HashComposedBinsPage*>(snapshot_writer_->get_intermediate_base());
  max_intermediates_ = snapshot_writer_->get_intermediate_size();
}

ErrorStack HashComposeContext::execute() {
  // Initializations
  std::memset(root_info_page_, 0, kPageSize);
  root_info_page_->header().storage_id_ = storage_id_;
  CHECK_ERROR(init_intermediates());
  CHECK_ERROR(init_cur_path());
  WRAP_ERROR_CODE(cur_bin_table_.create_memory(numa_node_));  // TASK(Hideaki) reuse memory
  cur_bin_table_.clean();
  VLOG(0) << "HashComposer-" << storage_id_ << " initialization done. processing...";

  bool processed_any = false;
  cur_bin_ = kCurBinNotOpened;
  while (true) {
    CHECK_ERROR(merge_sort_->next_batch());
    uint64_t count = merge_sort_->get_current_count();
    if (count == 0 && merge_sort_->is_ended_all()) {
      break;
    }
    processed_any = true;
    const snapshot::MergeSort::SortEntry* sort_entries = merge_sort_->get_sort_entries();
    uint64_t cur = 0;
    while (cur < count) {
      HashBin head_bin = sort_entries[cur].get_key();
      ASSERT_ND(head_bin < storage_.get_bin_count());
      if (cur_bin_ != head_bin) {
        // now we have to finalize the previous bin and switch to a new bin!
        ASSERT_ND(cur_bin_ < head_bin);  // sorted by bins
        CHECK_ERROR(close_cur_bin());
        ASSERT_ND(cur_bin_ == kCurBinNotOpened);
        CHECK_ERROR(open_cur_bin(head_bin));
        ASSERT_ND(cur_bin_ == head_bin);
      }

      // grab a range of logs that are in the same bin.
      uint64_t next;
      for (next = cur + 1U; LIKELY(next < count); ++next) {
        // this check uses sort_entries which are nicely contiguous.
        HashBin bin = sort_entries[next].get_key();
        ASSERT_ND(bin >= head_bin);
        if (UNLIKELY(bin != head_bin)) {
          break;
        }
      }

      WRAP_ERROR_CODE(apply_batch(cur, next));
      cur = next;
    }
    ASSERT_ND(cur == count);
  }

  if (!processed_any) {
    LOG(ERROR) << "wtf? no logs? storage-" << storage_id_;
  }

  CHECK_ERROR(finalize());
  return kRetOk;
}

ErrorCode HashComposeContext::apply_batch(uint64_t cur, uint64_t next) {
  // so far not much benefit of prefetching in hash composer.
  // the vast majority of the cost comes from hashinate.
  const uint16_t kFetchSize = 8;
  const log::RecordLogType* logs[kFetchSize];
  while (cur < next) {
    uint16_t desired = std::min<uint16_t>(kFetchSize, next - cur);
    uint16_t fetched = merge_sort_->fetch_logs(cur, desired, logs);
    for (uint16_t i = 0; i < kFetchSize && LIKELY(i < fetched); ++i) {
      const HashCommonLogType* log = reinterpret_cast<const HashCommonLogType*>(logs[i]);
      HashValue hash = hashinate(log->get_key(), log->key_length_);
      ASSERT_ND(cur_bin_ == (hash >> storage_.get_bin_shifts()));
      if (log->header_.get_type() == log::kLogCodeHashOverwrite) {
        CHECK_ERROR_CODE(cur_bin_table_.overwrite_record(
          log->header_.xct_id_,
          log->get_key(),
          log->key_length_,
          hash,
          log->get_payload(),
          log->payload_offset_,
          log->payload_count_));
      } else if (log->header_.get_type() == log::kLogCodeHashInsert) {
        CHECK_ERROR_CODE(cur_bin_table_.insert_record(
          log->header_.xct_id_,
          log->get_key(),
          log->key_length_,
          hash,
          log->get_payload(),
          log->payload_count_));
      } else {
        ASSERT_ND(log->header_.get_type() == log::kLogCodeHashDelete);
        CHECK_ERROR_CODE(cur_bin_table_.delete_record(
          log->header_.xct_id_,
          log->get_key(),
          log->key_length_,
          hash));
      }
    }
    cur += fetched;
    ASSERT_ND(cur <= next);
  }
  return kErrorCodeOk;
}

ErrorStack HashComposeContext::finalize() {
  ASSERT_ND(levels_ > 1U);

  CHECK_ERROR(close_cur_bin());

  // flush the main buffer. now we finalized all data pages
  if (allocated_pages_ > 0) {
    WRAP_ERROR_CODE(dump_data_pages());
    ASSERT_ND(allocated_pages_ == 0);
  }

  // as soon as we flush out all data pages, we can install snapshot pointers to them.
  // this is just about data pages (head pages in each bin), not intermediate pages
  uint64_t installed_count = 0;
  CHECK_ERROR(install_snapshot_data_pages(&installed_count));

  // then dump out HashComposedBinsPage.
  // we stored them in a separate buffer, and now finally we can get their page IDs.
  // Until now, we used relative indexes in intermediate buffer as page ID, storing them in
  // page ID header. now let's convert all of them to be final page ID.
  // base_pointer + offset in intermediate buffer will be the new page ID.
  const SnapshotPagePointer base_pointer = snapshot_writer_->get_next_page_id();
  for (uint32_t i = 0; i < root_children_; ++i) {
    // these are heads of linked-list. We keep pointers to these pages in root-info page
    root_info_page_->get_pointer(i).volatile_pointer_.clear();
    root_info_page_->get_pointer(i).snapshot_pointer_ = base_pointer + i;
  }
  for (uint32_t i = 0; i < allocated_intermediates_; ++i) {
    HashComposedBinsPage* page = intermediate_base_ + i;
    SnapshotPagePointer new_page_id = base_pointer + i;
    ASSERT_ND(page->header_.page_id_ == i);
    page->header_.page_id_ = new_page_id;
    if (page->next_page_) {
      // also updates next-pointers.
      ASSERT_ND(page->next_page_ < allocated_intermediates_);
      ASSERT_ND(page->bin_count_ == kHashComposedBinsPageMaxBins);
      page->next_page_ = base_pointer + page->next_page_;
    }
  }
  snapshot_writer_->dump_intermediates(0, allocated_intermediates_);

  return kRetOk;
}

ErrorCode HashComposeContext::dump_data_pages() {
  CHECK_ERROR_CODE(snapshot_writer_->dump_pages(0, allocated_pages_));
  ASSERT_ND(snapshot_writer_->get_next_page_id()
    == page_base_[0].header().page_id_ + allocated_pages_);
  ASSERT_ND(snapshot_writer_->get_next_page_id()
    == page_base_[allocated_pages_ - 1].header().page_id_ + 1ULL);
  allocated_pages_ = 0;
  return kErrorCodeOk;
}


inline HashDataPage* HashComposeContext::resolve_data(VolatilePagePointer pointer) const {
  return resolve_data_impl(volatile_resolver_, pointer);
}
inline HashIntermediatePage* HashComposeContext::resolve_intermediate(
  VolatilePagePointer pointer) const {
  return resolve_intermediate_impl(volatile_resolver_, pointer);
}

bool HashComposeContext::verify_new_pointer(SnapshotPagePointer pointer) const {
  ASSERT_ND(extract_local_page_id_from_snapshot_pointer(pointer) > 0U);
  if (!engine_->is_master()) {
    ASSERT_ND(extract_numa_node_from_snapshot_pointer(pointer) == numa_node_);
  }
  ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(pointer) == snapshot_id_);
  return true;
}
bool HashComposeContext::verify_old_pointer(SnapshotPagePointer pointer) const {
  return pointer == 0 || extract_snapshot_id_from_snapshot_pointer(pointer) != snapshot_id_;
}

///////////////////////////////////////////////////////////////////////
///
///  cur_path (snapshot pages in previous snapshot) related methods
///
///////////////////////////////////////////////////////////////////////
SnapshotPagePointer HashComposeContext::get_cur_path_bin_head(HashBin bin) const {
  ASSERT_ND(cur_path_valid_range_.contains(bin));
  if (cur_path_lowest_level_ > 0) {
    return 0;
  }
  ASSERT_ND(cur_path_[0].get_bin_range().contains(bin));
  uint16_t index = bin - cur_path_[0].get_bin_range().end_;
  return cur_path_[0].get_pointer(index).snapshot_pointer_;
}

ErrorStack HashComposeContext::init_cur_path() {
  if (previous_root_page_pointer_ == 0) {
    ASSERT_ND(is_initial_snapshot());
    std::memset(cur_path_, 0, kPageSize * levels_);
    cur_path_lowest_level_ = 0;
    cur_path_valid_range_ = HashBinRange(0, 0);
  } else {
    ASSERT_ND(!is_initial_snapshot());
    HashIntermediatePage* root = get_cur_path_page(levels_ - 1U);
    WRAP_ERROR_CODE(previous_snapshot_files_->read_page(previous_root_page_pointer_, root));
    ASSERT_ND(root->header().storage_id_ == storage_id_);
    ASSERT_ND(root->header().page_id_ == previous_root_page_pointer_);
    ASSERT_ND(root->get_level() + 1U == levels_);
    ASSERT_ND(root->get_bin_range() == HashBinRange(0ULL, kHashMaxBins[levels_]));
    cur_path_lowest_level_ = root->get_level();
    cur_path_valid_range_ = root->get_bin_range();

    HashIntermediatePage* parent = root;
    while (parent->get_level() > 0) {
      HashIntermediatePage* child = get_cur_path_page(parent->get_level() - 1U);
      SnapshotPagePointer pointer = parent->get_pointer(0).snapshot_pointer_;
      if (pointer == 0) {
        std::memset(child, 0, kPageSize);
        break;
      } else {
        WRAP_ERROR_CODE(previous_snapshot_files_->read_page(pointer, child));
        ASSERT_ND(child->header().storage_id_ == storage_id_);
        ASSERT_ND(child->header().page_id_ == pointer);
        ASSERT_ND(child->get_level() + 1U == parent->get_level());
        ASSERT_ND(child->get_bin_range() == HashBinRange(0ULL, parent->get_level()));
        cur_path_lowest_level_ = child->get_level();
        cur_path_valid_range_ = child->get_bin_range();
        parent = child;
      }
    }
  }
  return kRetOk;
}

inline ErrorCode HashComposeContext::update_cur_path_if_needed(HashBin bin) {
  ASSERT_ND(verify_cur_path());

  // Even when LIKELY mis-predicts, the penalty is amortized by the page-read cost.
  if (LIKELY(is_initial_snapshot()
    || levels_ == 1U
    || (cur_path_valid_range_.contains(bin) && cur_path_lowest_level_ == 0))) {
    return kErrorCodeOk;
  }

  return update_cur_path(bin);
}

ErrorCode HashComposeContext::update_cur_path(HashBin bin) {
  ASSERT_ND(!is_initial_snapshot());
  ASSERT_ND(!cur_path_valid_range_.contains(bin));
  ASSERT_ND(levels_ > 1U);  // otherwise no page switch should happen
  ASSERT_ND(verify_cur_path());

  // goes up until cur_path_valid_range_.contains(bin)
  while (!cur_path_valid_range_.contains(bin)) {
    ASSERT_ND(cur_path_lowest_level_ + 1U < levels_);  // otherwise even root doesn't contain it
    ++cur_path_lowest_level_;
    cur_path_valid_range_ = get_cur_path_lowest()->get_bin_range();
    ASSERT_ND(get_cur_path_lowest()->get_bin_range() == cur_path_valid_range_);
  }

  // then goes down as much as possible
  IntermediateRoute route = IntermediateRoute::construct(bin);

#ifndef NDEBUG
  // route[level+1] is the ordinal in intermediate page of the level+1, pointing to the child.
  // thus cur_path[level] should have that pointer as its page ID.
  for (uint8_t level = cur_path_lowest_level_; level + 1U < levels_; ++level) {
    SnapshotPagePointer child_id = get_cur_path_page(level)->header().page_id_;
    HashIntermediatePage* parent = get_cur_path_page(level + 1U);
    ASSERT_ND(parent->get_pointer(route.route[level + 1U]).snapshot_pointer_ == child_id);
  }
#endif  // NDEBUG

  while (cur_path_lowest_level_ > 0) {
    uint8_t index = route.route[cur_path_lowest_level_];
    HashIntermediatePage* page = get_cur_path_page(cur_path_lowest_level_);
    SnapshotPagePointer pointer = page->get_pointer(index).snapshot_pointer_;
    if (pointer == 0) {
      // the page doesn't exist in previous snapshot. that's fine.
      break;
    } else {
      HashIntermediatePage* child = get_cur_path_page(cur_path_lowest_level_ + 1U);
      CHECK_ERROR_CODE(previous_snapshot_files_->read_page(pointer, child));
      ASSERT_ND(child->header().storage_id_ == storage_id_);
      ASSERT_ND(child->header().page_id_ == pointer);
      ASSERT_ND(child->get_level() + 1U == cur_path_lowest_level_);
      ASSERT_ND(child->get_bin_range().contains(bin));
      cur_path_lowest_level_ = child->get_level();
      cur_path_valid_range_ = child->get_bin_range();
    }
  }

  ASSERT_ND(cur_path_valid_range_.contains(bin));
  ASSERT_ND(get_cur_path_lowest()->get_bin_range() == cur_path_valid_range_);
  ASSERT_ND(verify_cur_path());
  return kErrorCodeOk;
}

bool HashComposeContext::verify_cur_path() const {
  if (is_initial_snapshot()) {
    ASSERT_ND(cur_path_lowest_level_ == levels_);
  } else {
    ASSERT_ND(cur_path_lowest_level_ < levels_);
  }
  for (uint8_t level = cur_path_lowest_level_; level < kHashMaxLevels; ++level) {
    if (level >= levels_) {
      ASSERT_ND(cur_path_[level].header().page_id_ == 0);
      continue;
    }
    ASSERT_ND(cur_path_[level].header().page_id_ != 0);
    ASSERT_ND(cur_path_[level].get_level() == level);
    ASSERT_ND(cur_path_[level].header().storage_id_ == storage_id_);
    if (level > cur_path_lowest_level_) {
      HashBinRange range = cur_path_[level].get_bin_range();
      HashBinRange child_range = cur_path_[level - 1U].get_bin_range();
      ASSERT_ND(range.contains(child_range));
    }
  }

  return true;
}

///////////////////////////////////////////////////////////////////////
///
///  cur_bin_ (the records being modified for current bin) related methods
///
///////////////////////////////////////////////////////////////////////
ErrorStack HashComposeContext::close_cur_bin() {
  if (cur_bin_ == kCurBinNotOpened) {
    // already closed
    return kRetOk;
  }

  // construct new data pages from cur_bin_table_.
  // to make sure we have enough pages in the main buffer,
  // we flush pages so far via a very conservative estimate.
  // assuming each bin receives a small number of records, this doesn't harm anything.
  uint32_t physical_records = cur_bin_table_.get_physical_record_count();
  if (UNLIKELY(physical_records > 1000U)) {
    LOG(WARNING) << "A hash bin has more than 1000 records?? That's an unexpected usage."
      << " There is either a skew or mis-sizing.";
  }
  uint64_t remaining_buffer = allocated_pages_ - max_pages_;
  if (UNLIKELY(remaining_buffer < physical_records)) {  // super-conservative. one-record per page.
    WRAP_ERROR_CODE(dump_data_pages());
  }

  const SnapshotPagePointer base_pointer = snapshot_writer_->get_next_page_id();
  HashDataPage* head_page = page_base_ + allocated_pages_;
  SnapshotPagePointer head_page_id = base_pointer + allocated_pages_;
  head_page->initialize_snapshot_page(storage_id_, head_page_id, cur_bin_);
  ++allocated_pages_;
  ASSERT_ND(allocated_pages_ <= max_pages_);

  HashDataPage* cur_page = head_page;
  const uint32_t begin = cur_bin_table_.get_first_record();
  const uint32_t end = cur_bin_table_.get_records_consumed();
  for (uint32_t i = begin; i < end; ++i) {
    HashTmpBin::Record* record = cur_bin_table_.get_record(i);
    ASSERT_ND(cur_bin_ == (record->hash_ >> storage_.get_bin_shifts()));
    if (record->xct_id_.is_deleted()) {
      continue;
    }
    uint16_t available = cur_page->available_space();
    uint16_t required = cur_page->required_space(record->key_length_, record->payload_length_);
    if (available < required) {
      // move on to next page
      SnapshotPagePointer page_id = base_pointer + allocated_pages_;
      HashDataPage* next_page = page_base_ + allocated_pages_;
      next_page->initialize_snapshot_page(storage_id_, page_id, cur_bin_);
      cur_page->next_page_address()->snapshot_pointer_ = page_id;
      cur_page = next_page;

      ++allocated_pages_;
      ASSERT_ND(allocated_pages_ <= max_pages_);
    }

    BloomFilterFingerprint fingerprint = DataPageBloomFilter::extract_fingerprint(record->hash_);
    cur_page->create_record_in_snapshot(
      record->xct_id_,
      record->hash_,
      fingerprint,
      record->get_key(),
      record->key_length_,
      record->get_payload(),
      record->payload_length_);
  }

  // finally, register the head page in intermediate page. the bin is now closed.
  WRAP_ERROR_CODE(append_to_intermediate(head_page_id, cur_bin_));
  cur_bin_ = kCurBinNotOpened;

  return kRetOk;
}

ErrorStack HashComposeContext::open_cur_bin(HashBin bin) {
  ASSERT_ND(cur_bin_ == kCurBinNotOpened);
  // switch to an intermediate page containing this bin
  WRAP_ERROR_CODE(update_cur_path_if_needed(bin));

  cur_bin_table_.clean_quick();

  // Load-up the cur_bin_table_ with existing records in previous snapshot
  SnapshotPagePointer page_id = get_cur_path_bin_head(bin);
  while (page_id) {
    HashDataPage* page = reinterpret_cast<HashDataPage*>(data_page_io_memory_.get_block());
    // hopefully, most of this read will be sequential. so underlying HW will pre-fetch and cache
    WRAP_ERROR_CODE(previous_snapshot_files_->read_page(page_id, page));
    ASSERT_ND(page->header().storage_id_ == storage_id_);
    ASSERT_ND(page->header().page_id_ == page_id);
    ASSERT_ND(page->get_bin() == bin);
    ASSERT_ND(page->next_page().volatile_pointer_.is_null());
    uint16_t records = page->get_record_count();
    for (uint16_t i = 0; i < records; ++i) {
      const HashDataPage::Slot& slot = page->get_slot(i);
      ASSERT_ND(!slot.tid_.xct_id_.is_deleted());
      ASSERT_ND(!slot.tid_.xct_id_.is_moved());
      ASSERT_ND(!slot.tid_.xct_id_.is_being_written());
      const char* data = page->record_from_offset(slot.offset_);
      WRAP_ERROR_CODE(cur_bin_table_.insert_record(
        slot.tid_.xct_id_,
        data,
        slot.key_length_,
        slot.hash_,
        data + slot.get_aligned_key_length(),
        slot.payload_length_));
    }
    page_id = page->next_page().snapshot_pointer_;
  }

  cur_bin_ = bin;
  return kRetOk;
}

///////////////////////////////////////////////////////////////////////
///
///  HashComposedBinsPage (snapshot's intermediate) related methods
///
///////////////////////////////////////////////////////////////////////
ErrorStack HashComposeContext::init_intermediates() {
  ASSERT_ND(allocated_intermediates_ == 0);
  ASSERT_ND(intermediate_base_
    == reinterpret_cast<HashComposedBinsPage*>(snapshot_writer_->get_intermediate_base()));
  uint16_t count = storage_.get_root_children();
  if (max_intermediates_ < count) {
    return ERROR_STACK_MSG(kErrorCodeInternalError, "max_intermediates weirdly too small");
  }

  std::memset(intermediate_base_, 0, kPageSize * count);
  for (uint16_t i = 0; i < count; ++i) {
    SnapshotPagePointer new_page_id = allocated_intermediates_;
    ++allocated_intermediates_;
    intermediate_base_[i].header_.page_id_ = new_page_id;
    intermediate_base_[i].header_.page_type_ = kHashComposedBinsPageType;
    uint64_t interval = kHashMaxBins[levels_ - 1U];
    HashBinRange range(i * interval, (i + 1U) * interval);
    intermediate_base_[i].bin_range_ = range;
  }

  return kRetOk;
}

HashComposedBinsPage* HashComposeContext::get_intermediate_tail(uint8_t root_index) const {
  HashComposedBinsPage* page = get_intermediate_head(root_index);
  while (true) {
    ASSERT_ND(page);
    ASSERT_ND(intermediate_base_ + page->header_.page_id_ == page);
    ASSERT_ND(page->header_.get_page_type() == kHashComposedBinsPageType);
    if (page->next_page_ == 0) {
      return page;
    }
    page = intermediate_base_ + page->next_page_;
  }
}

inline void HashComposeContext::update_cur_intermediate_tail_if_needed(HashBin bin) {
  ASSERT_ND(bin < total_bin_count_);
  if (LIKELY(cur_intermediate_tail_ && cur_intermediate_tail_->bin_range_.contains(bin))) {
    return;
  }
  update_cur_intermediate_tail(bin);
}


void HashComposeContext::update_cur_intermediate_tail(HashBin bin) {
  ASSERT_ND(!cur_intermediate_tail_ || !cur_intermediate_tail_->bin_range_.contains(bin));
  IntermediateRoute route = IntermediateRoute::construct(bin);
  uint8_t root_index = route.route[levels_ - 1U];
  cur_intermediate_tail_ = get_intermediate_tail(root_index);
  ASSERT_ND(cur_intermediate_tail_->bin_range_.contains(bin));
}

ErrorCode HashComposeContext::append_to_intermediate(SnapshotPagePointer page_id, HashBin bin) {
  update_cur_intermediate_tail_if_needed(bin);
  ASSERT_ND(cur_intermediate_tail_->bin_range_.contains(bin));
  ASSERT_ND(cur_intermediate_tail_->bin_count_ <= kHashComposedBinsPageMaxBins);
  ASSERT_ND(cur_intermediate_tail_->next_page_ == 0);
  if (cur_intermediate_tail_->bin_count_ == kHashComposedBinsPageMaxBins) {
    // Now we need to append a new intermediate page.
    DVLOG(1) << "Growing intermediate page in hash composer...";
    CHECK_ERROR_CODE(expand_intermediate_pool_if_needed());
    SnapshotPagePointer next_page_id = allocated_intermediates_;
    HashComposedBinsPage* next = intermediate_base_ + next_page_id;
    std::memset(next, 0, kPageSize);
    next->header_.page_id_ = next_page_id;
    next->bin_range_ = cur_intermediate_tail_->bin_range_;
    ++allocated_intermediates_;
    cur_intermediate_tail_->next_page_ = next_page_id;
    cur_intermediate_tail_ = next;
    ASSERT_ND(cur_intermediate_tail_->next_page_ == 0);
    ASSERT_ND(cur_intermediate_tail_->bin_range_.contains(bin));
  }

  ASSERT_ND(cur_intermediate_tail_->bin_count_ < kHashComposedBinsPageMaxBins);
  uint8_t index = cur_intermediate_tail_->bin_count_;
  // hash-bin should be fully sorted
  ASSERT_ND(index == 0 || cur_intermediate_tail_->bins_[index - 1U].bin_ < bin);
  ComposedBin& entry = cur_intermediate_tail_->bins_[index];
  entry.page_id_ = page_id;
  entry.bin_ = bin;
  ++cur_intermediate_tail_->bin_count_;
  return kErrorCodeOk;
}

ErrorCode HashComposeContext::expand_intermediate_pool_if_needed() {
  ASSERT_ND(allocated_intermediates_ <= max_intermediates_);
  if (UNLIKELY(allocated_intermediates_ == max_intermediates_)) {
    LOG(INFO) << "Automatically expanding intermediate_pool. This should be a rare event";
    uint32_t required = allocated_intermediates_ + 1U;
    CHECK_ERROR_CODE(snapshot_writer_->expand_intermediate_memory(required, true));
    intermediate_base_
      = reinterpret_cast<HashComposedBinsPage*>(snapshot_writer_->get_intermediate_base());
    max_intermediates_ = snapshot_writer_->get_intermediate_size();
  }
  return kErrorCodeOk;
}

///////////////////////////////////////////////////////////////////////
///
///  HashComposeContext::install_snapshot_data_pages() related methods
///
///////////////////////////////////////////////////////////////////////
ErrorStack HashComposeContext::install_snapshot_data_pages(uint64_t* installed_count) const {
  *installed_count = 0;
  VolatilePagePointer pointer = storage_.get_control_block()->root_page_pointer_.volatile_pointer_;
  if (pointer.is_null()) {
    VLOG(0) << "No volatile pages.. maybe while restart?";
    return kRetOk;
  }

  HashIntermediatePage* volatile_root = resolve_intermediate(pointer);

  debugging::StopWatch watch;
  for (uint8_t root_child = 0; root_child < root_children_; ++root_child) {
    VolatilePagePointer child_pointer = volatile_root->get_pointer(root_child).volatile_pointer_;
    if (child_pointer.is_null()) {
      LOG(WARNING) << "Um, the subtree doesn't exist? how come. but fine";
      continue;
    }

    const HashComposedBinsPage* composed = get_intermediate_head(root_child);
    if (levels_ == 1U) {
      // the root child is already a data page. We should have at most one pointer to install!
      ASSERT_ND(volatile_root->get_level() == 0);
      ASSERT_ND(composed->next_page_ == 0);
      ASSERT_ND(composed->bin_count_ == 0 || composed->bin_count_ == 1U);
      if (composed->bin_count_ > 0) {
        ASSERT_ND(composed->bins_[0].bin_ == root_child);
        ASSERT_ND(verify_new_pointer(composed->bins_[0].page_id_));
        ASSERT_ND(verify_old_pointer(volatile_root->get_pointer(root_child).snapshot_pointer_));
        volatile_root->get_pointer(root_child).snapshot_pointer_ = composed->bins_[0].page_id_;
      }
    } else {
      ASSERT_ND(volatile_root->get_level() > 0);
      HashIntermediatePage* volatile_child = resolve_intermediate(child_pointer);
      ASSERT_ND(volatile_child->header().get_page_type() == kHashIntermediatePageType);
      CHECK_ERROR(install_snapshot_data_pages_root_child(
        composed,
        volatile_child,
        installed_count));
    }
  }
  watch.stop();
  VLOG(0) << "HashStorage-" << storage_id_ << " installed " << *installed_count << " pointers"
    << " to data pages in " << watch.elapsed_ms() << "ms";
  return kRetOk;
}

ErrorStack HashComposeContext::install_snapshot_data_pages_root_child(
  const HashComposedBinsPage* composed,
  HashIntermediatePage* volatile_root_child,
  uint64_t* installed_count) const {
  typedef HashIntermediatePage* PagePtr;
  PagePtr volatile_path[kHashMaxLevels];  // the traversal path in volatile world
  std::memset(volatile_path, 0, sizeof(volatile_path));
  volatile_path[volatile_root_child->get_level()] = volatile_root_child;
  uint8_t volatile_path_lowest_level = volatile_root_child->get_level();

  const HashComposedBinsPage* page = composed;
  HashBin previous_bin = kCurBinNotOpened;
  while (true) {
    for (uint16_t i = 0; i < page->bin_count_; ++i) {
      // entries should be sorted by bins
      HashBin bin = page->bins_[i].bin_;
      ASSERT_ND(previous_bin == kCurBinNotOpened || previous_bin < bin);
      previous_bin = bin;
      ASSERT_ND(volatile_root_child->get_bin_range().contains(bin));

      // go back to upper levels first,
      while (UNLIKELY(!volatile_path[volatile_path_lowest_level]->get_bin_range().contains(bin))) {
        ++volatile_path_lowest_level;
        ASSERT_ND(volatile_path_lowest_level <= volatile_root_child->get_level());
      }

      // then go down to hit the exact level-0 volatile page
      while (UNLIKELY(volatile_path_lowest_level > 0)) {
        PagePtr cur = volatile_path[volatile_path_lowest_level];
        ASSERT_ND(cur->get_level() == volatile_path_lowest_level);
        const HashBinRange& range = cur->get_bin_range();
        ASSERT_ND(range.contains(bin));
        uint16_t index = (bin - range.begin_) / kHashMaxBins[volatile_path_lowest_level];
        ASSERT_ND(verify_old_pointer(cur->get_pointer(index).snapshot_pointer_));
        VolatilePagePointer pointer = cur->get_pointer(index).volatile_pointer_;
        // something was changed in this subtree, we sure have a volatile page here.
        ASSERT_ND(!pointer.is_null());
        PagePtr next = resolve_intermediate(pointer);
        ASSERT_ND(next->get_bin_range().contains(bin));
        ASSERT_ND(next->get_level() + 1U == volatile_path_lowest_level);
        --volatile_path_lowest_level;
        volatile_path[volatile_path_lowest_level] = next;
      }

      PagePtr bottom = volatile_path[0];
      ASSERT_ND(volatile_path_lowest_level == 0 && bottom->get_bin_range().contains(bin));
      uint16_t index = bin - bottom->get_bin_range().begin_;
      ASSERT_ND(!bottom->get_pointer(index).volatile_pointer_.is_null());
      ASSERT_ND(verify_old_pointer(bottom->get_pointer(index).snapshot_pointer_));
      ASSERT_ND(verify_new_pointer(page->bins_[i].page_id_));
      bottom->get_pointer(index).snapshot_pointer_ = page->bins_[i].page_id_;
      ++(*installed_count);
    }

    ASSERT_ND(page->next_page_ < allocated_intermediates_);
    if (page->next_page_ == 0) {
      break;
    } else {
      page = intermediate_base_ + page->next_page_;
    }
  }

  return kRetOk;
}


/////////////////////////////////////////////////////////////////////////////
///
///  drop_volatiles and related methods
///
/////////////////////////////////////////////////////////////////////////////
Composer::DropResult HashComposer::drop_volatiles(const Composer::DropVolatilesArguments& args) {
  Composer::DropResult result(args);
  if (storage_.get_hash_metadata()->keeps_all_volatile_pages()) {
    LOG(INFO) << "Keep-all-volatile: Storage-" << storage_.get_name()
      << " is configured to keep all volatile pages.";
    result.dropped_all_ = false;
    return result;
  }

  DualPagePointer* root_pointer = &storage_.get_control_block()->root_page_pointer_;
  HashIntermediatePage* volatile_page = resolve_intermediate(root_pointer->volatile_pointer_);
  if (volatile_page == nullptr) {
    LOG(INFO) << "No volatile root page. Probably while restart";
    return result;
  }

  // We iterate through all existing volatile pages to drop volatile pages of
  // level-3 or deeper (if the storage has only 2 levels, keeps all).
  // this "level-3 or deeper" is a configuration per storage.
  // Even if the volatile page is deeper than that, we keep them if it contains newer modification,
  // including descendants (so, probably we will keep higher levels anyways).
  uint16_t count = storage_.get_root_children();
  for (uint16_t i = 0; i < count; ++i) {
    DualPagePointer& child_pointer = volatile_page->get_pointer(i);
    if (!child_pointer.volatile_pointer_.is_null()) {
      ASSERT_ND(child_pointer.snapshot_pointer_ != 0);
      uint16_t partition = extract_numa_node_from_snapshot_pointer(child_pointer.snapshot_pointer_);
      if (!args.partitioned_drop_ || partition == args.my_partition_) {
        result.combine(drop_volatiles_recurse(args, &child_pointer));
      }
    }
  }
  // root page is kept at this point in this case. we need to check with other threads
  return result;
}

void HashComposer::drop_root_volatile(const Composer::DropVolatilesArguments& args) {
  if (storage_.get_hash_metadata()->keeps_all_volatile_pages()) {
    LOG(INFO) << "Oh, but keep-all-volatile is on. Storage-" << storage_.get_name()
      << " is configured to keep all volatile pages.";
    return;
  }
  if (is_to_keep_volatile(storage_.get_levels() - 1U)) {
    LOG(INFO) << "Oh, but Storage-" << storage_.get_name() << " is configured to keep"
      << " the root page.";
    return;
  }
  DualPagePointer* root_pointer = &storage_.get_control_block()->root_page_pointer_;
  HashIntermediatePage* volatile_page = resolve_intermediate(root_pointer->volatile_pointer_);
  if (volatile_page == nullptr) {
    LOG(INFO) << "Oh, but root volatile page already null";
    return;
  }

  LOG(INFO) << "Okay, drop em all!!";
  drop_all_recurse(args, root_pointer);
}

void HashComposer::drop_all_recurse(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer) {
  if (pointer->volatile_pointer_.is_null()) {
    return;
  }
  HashIntermediatePage* page = resolve_intermediate(pointer->volatile_pointer_);
  for (uint16_t i = 0; i < kHashIntermediatePageFanout; ++i) {
    DualPagePointer* child_pointer = page->get_pointer_address(i);
    if (page->get_level() > 0) {
      drop_all_recurse(args, child_pointer);
    } else {
      drop_volatile_entire_bin(args, child_pointer);
    }
  }
  args.drop(engine_, pointer->volatile_pointer_);
  pointer->volatile_pointer_.clear();
}

void HashComposer::drop_volatile_entire_bin(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer_to_head) const {
  // drop data pages in a bin. this must be a while loop, not recursion.
  // otherwise, when there is a rare case of long linked list, outofstack!
  // be careful to not drop the current page before getting required information
  VolatilePagePointer cur_pointer = pointer_to_head->volatile_pointer_;
  while (!cur_pointer.is_null()) {
    HashDataPage* cur = resolve_data(cur_pointer);
    VolatilePagePointer next_pointer = cur->next_page().volatile_pointer_;
    args.drop(engine_, cur_pointer);  // notice, we drop AFTER retrieving next_pointer
    cur_pointer = next_pointer;
  }
  pointer_to_head->volatile_pointer_.clear();
}

inline Composer::DropResult HashComposer::drop_volatiles_recurse(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer) {
  ASSERT_ND(pointer->snapshot_pointer_ == 0
    || extract_snapshot_id_from_snapshot_pointer(pointer->snapshot_pointer_)
        != snapshot::kNullSnapshotId);
  // The snapshot pointer CAN be null.
  // It means that this subtree has not constructed a new snapshot page in this snapshot.

  Composer::DropResult result(args);
  HashIntermediatePage* page = resolve_intermediate(pointer->volatile_pointer_);

  // Explore/replace children first because we need to know if there is new modification.
  // In that case, we must keep this volatile page, too.
  // Intermediate volatile page is kept iff there are no child volatile pages.
  for (uint16_t i = 0; i < kHashIntermediatePageFanout; ++i) {
    DualPagePointer* child_pointer = page->get_pointer_address(i);
    if (!child_pointer->volatile_pointer_.is_null()) {
      if (page->get_level() > 0) {
        result.combine(drop_volatiles_recurse(args, child_pointer));
      } else {
        if (can_drop_volatile_bin(
          child_pointer->volatile_pointer_,
          args.snapshot_.valid_until_epoch_)) {
          drop_volatile_entire_bin(args, child_pointer);
        } else {
          result.dropped_all_ = false;
          // in hash composer, we currently do not emit accurate information on this.
          // we are not using this information anyway.
          result.max_observed_ = args.snapshot_.valid_until_epoch_.one_more();
        }
      }
    }
  }
  if (result.dropped_all_) {
    if (is_to_keep_volatile(page->get_level())) {
      DVLOG(2) << "Exempted";
      result.dropped_all_ = false;
    } else {
      args.drop(engine_, pointer->volatile_pointer_);
      pointer->volatile_pointer_.clear();
    }
  } else {
    DVLOG(1) << "Couldn't drop an intermediate page that has a recent modification in child";
  }
  ASSERT_ND(!result.dropped_all_ || pointer->volatile_pointer_.is_null());
  return result;
}

bool HashComposer::can_drop_volatile_bin(VolatilePagePointer head, Epoch valid_until) const {
  for (HashDataPage* cur = resolve_data(head);
        cur;
        cur = resolve_data(cur->next_page().volatile_pointer_)) {
    for (uint16_t i = 0; i < cur->get_record_count(); ++i) {
      Epoch epoch = cur->get_slot(i).tid_.xct_id_.get_epoch();
      if (epoch > valid_until) {
        return false;
      }
    }
  }

  return true;
}


inline bool HashComposer::is_to_keep_volatile(uint16_t level) {
  /*
  uint16_t threshold = storage_.get_hash_metadata()->snapshot_drop_volatile_pages_threshold_;
  uint16_t hash_levels = storage_.get_levels();
  ASSERT_ND(level < hash_levels);
  // examples:
  // when threshold=0, all levels (0~hash_levels-1) should return false.
  // when threshold=1, only root level (hash_levels-1) should return true
  // when threshold=2, upto hash_levels-2..
  return threshold + level >= hash_levels;
  */
  return level + 2U > storage_.get_levels();  // TASK(Hideaki) should be a config
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
