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
#include <gtest/gtest.h>

#include <memory>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace array {
DEFINE_TEST_CASE_PACKAGE(ArrayPartitionerTest, foedus.storage.array);

ErrorStack populate(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  ArrayStorage array(args.engine_, "test");
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  char buf[4096];
  for (uint32_t i = 0; i < array.get_array_size() / 2U; ++i) {
    ArrayOffset rec = i;
    if (context->get_numa_node() != 0) {
      rec += array.get_array_size() / 2U;
    }
    CHECK_ERROR(array.get_record(context, rec, buf));
  }
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

TEST(ArrayPartitionerTest, InitialPartition) {
  if (!is_multi_nodes()) {
    return;
  }
  EngineOptions options = get_tiny_options();
  options.log_.log_buffer_kb_ = 1 << 10;
  options.thread_.group_count_ = 2;
  options.memory_.page_pool_size_mb_per_node_ = 4;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("populate", populate);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayStorage out;
    Epoch commit_epoch;
    ArrayMetadata meta("test", 3000, 300);  // 1 record per page. 300 leaf pages.
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_on_numa_node_synchronous(0, "populate"));
    COERCE_ERROR(engine.get_thread_pool()->impersonate_on_numa_node_synchronous(1, "populate"));
    VolatilePagePointer root_ptr = out.get_control_block()->root_page_pointer_.volatile_pointer_;
    EXPECT_EQ(0, root_ptr.components.numa_node);
    const memory::GlobalVolatilePageResolver& resolver
      = engine.get_memory_manager()->get_global_volatile_page_resolver();
    ArrayPage* root = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(root_ptr));
    EXPECT_EQ(0, root->get_interior_record(0).volatile_pointer_.components.numa_node);
    EXPECT_EQ(1U, root->get_interior_record(1).volatile_pointer_.components.numa_node);
    ArrayPage* left = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(
      root->get_interior_record(0).volatile_pointer_));
    ArrayPage* right = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(
      root->get_interior_record(1).volatile_pointer_));
    for (uint16_t i = 0; i < 150; ++i) {
      EXPECT_EQ(0, left->get_interior_record(i).volatile_pointer_.components.numa_node) << i;
    }
    for (uint16_t i = 150; i < kInteriorFanout; ++i) {
      EXPECT_EQ(1U, left->get_interior_record(i).volatile_pointer_.components.numa_node) << i;
    }
    for (uint16_t i = 0; i < 300U - kInteriorFanout; ++i) {
      EXPECT_EQ(1U, right->get_interior_record(i).volatile_pointer_.components.numa_node) << i;
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

const uint16_t kPayload = 16;

typedef void (*TestFunctor)(Partitioner partitioner);
void execute_test(TestFunctor functor, uint64_t array_size = 1024) {
  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = 2;  // otherwise we can't test partitioning
  Engine engine(options);
  engine.get_proc_manager()->pre_register("populate", populate);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayStorage out;
    Epoch commit_epoch;
    ArrayMetadata meta("test", kPayload, array_size);
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_on_numa_node_synchronous(0, "populate"));
    COERCE_ERROR(engine.get_thread_pool()->impersonate_on_numa_node_synchronous(1, "populate"));
    Partitioner partitioner(&engine, out.get_id());
    memory::AlignedMemory work_memory;
    work_memory.alloc(1U << 21, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
    cache::SnapshotFileSet fileset(&engine);
    COERCE_ERROR(fileset.initialize())
    Partitioner::DesignPartitionArguments args = { &work_memory, &fileset};
    COERCE_ERROR(partitioner.design_partition(args));
    COERCE_ERROR(fileset.uninitialize());
    EXPECT_TRUE(partitioner.is_valid());
    functor(partitioner);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

void EmptyFunctor(Partitioner partitioner) {
  snapshot::BufferPosition dummy[1];
  snapshot::LogBuffer log_buffer(nullptr);
  PartitionId results[1];
  Partitioner::PartitionBatchArguments args = { 0, log_buffer, dummy, 0, results };
  partitioner.partition_batch(args);
}
TEST(ArrayPartitionerTest, Empty) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&EmptyFunctor);
}

template <int LOG_COUNT>
struct Logs {
  explicit Logs(Partitioner partitioner)
  : log_buffer_(memory_), partitioner_(partitioner), cur_pos_(0), cur_count_(0) {
    std::memset(memory_, 0xDA, LOG_COUNT * 64);  // fill with garbage
    sort_buffer_.alloc(LOG_COUNT * 16, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  }

  void add_log(Epoch::EpochInteger epoch_int, uint16_t ordinal,
               ArrayOffset offset, uint16_t payload_offset, uint16_t payload_count) {
    ArrayOverwriteLogType* entry = reinterpret_cast<ArrayOverwriteLogType*>(memory_ + cur_pos_);
    char dummy[kPayload];
    entry->populate(partitioner_.get_storage_id(), offset, dummy, payload_offset, payload_count);
    entry->header_.xct_id_.set(epoch_int, ordinal);
    positions_[cur_count_] = log_buffer_.compact(entry);
    cur_pos_ += entry->header_.log_length_;
    ++cur_count_;
  }

  void partition_batch() {
    Partitioner::PartitionBatchArguments args = {
      0,
      log_buffer_,
      positions_,
      cur_count_,
      partition_results_};
    partitioner_.partition_batch(args);
  }
  uint32_t sort_batch(Epoch::EpochInteger base_epoch) {
    uint32_t written_count = 0;
    Partitioner::SortBatchArguments args = {
      log_buffer_,
      positions_,
      cur_count_,
      0,
      0,
      &sort_buffer_,
      Epoch(base_epoch),
      sort_results_,
      &written_count};
    partitioner_.sort_batch(args);
    return written_count;
  }

  char   memory_[LOG_COUNT * 64];
  snapshot::LogBuffer log_buffer_;
  Partitioner   partitioner_;
  uint64_t cur_pos_;
  uint32_t cur_count_;
  snapshot::BufferPosition positions_[LOG_COUNT];
  PartitionId partition_results_[LOG_COUNT];
  snapshot::BufferPosition sort_results_[LOG_COUNT];
  memory::AlignedMemory sort_buffer_;
};

void PartitionBasicFunctor(Partitioner partitioner) {
  std::unique_ptr< Logs<16> > logs(new Logs<16>(partitioner));
  for (int i = 0; i < 16; ++i) {
    logs->add_log(2, i, 123 - i, 4, 8);
  }
  logs->partition_batch();
}

TEST(ArrayPartitionerTest, PartitionBasic) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&PartitionBasicFunctor);
}


void SortBasicFunctor(Partitioner partitioner) {
  std::unique_ptr< Logs<16> > logs(new Logs<16>(partitioner));
  for (int i = 0; i < 16; ++i) {
    // epoch/ordinal is ordered, but offsets are reverse-ordered
    logs->add_log(2 + i, i, 123 - i, 4, 8);
  }
  EXPECT_EQ(16, logs->sort_batch(2));
  for (int i = 0; i < 16; ++i) {
    // results should be reverse-ordered
    EXPECT_EQ(logs->positions_[15 - i], logs->sort_results_[i]);
  }
}

TEST(ArrayPartitionerTest, SortBasic) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&SortBasicFunctor);
}

void SortCompactFunctor(Partitioner partitioner) {
  std::unique_ptr< Logs<16> > logs(new Logs<16>(partitioner));
  // all of them are on the same key and the same data region.
  // all but the last log should be discarded away.
  for (int i = 0; i < 16; ++i) {
    // epoch is ordered, but ordinal is reverse-ordered.
    logs->add_log(2 + i, 30 - i, 123, 4, 8);
  }
  EXPECT_EQ(1, logs->sort_batch(2));
  // as epoch is more significant than ordinal, [15] is the last log
  EXPECT_EQ(logs->positions_[15], logs->sort_results_[0]);
}

TEST(ArrayPartitionerTest, SortCompact) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&SortCompactFunctor);
}

void SortNoCompactFunctor(Partitioner partitioner) {
  std::unique_ptr< Logs<16> > logs(new Logs<16>(partitioner));
  // Again on the same key, but data regions are different.
  for (int i = 0; i < 16; ++i) {
    logs->add_log(2 + i, 30 - i, 123, i, 1);
  }
  // all of them should not be compacted, and sorted by epoch
  EXPECT_EQ(16, logs->sort_batch(2));
  for (int i = 0; i < 16; ++i) {
    EXPECT_EQ(logs->positions_[i], logs->sort_results_[i]);
  }
}

TEST(ArrayPartitionerTest, SortNoCompact) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&SortNoCompactFunctor);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(ArrayPartitionerTest, foedus.storage.array);
