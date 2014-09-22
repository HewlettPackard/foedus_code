/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <memory>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace array {
DEFINE_TEST_CASE_PACKAGE(ArrayPartitionerTest, foedus.storage.array);

TEST(ArrayPartitionerTest, InitialPartition) {
  if (!is_multi_nodes()) {
    return;
  }
  EngineOptions options = get_tiny_options();
  options.log_.log_buffer_kb_ = 1 << 10;
  options.thread_.group_count_ = 2;
  options.memory_.page_pool_size_mb_per_node_ = 4;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayStorage out;
    Epoch commit_epoch;
    ArrayMetadata meta("test5", 3000, 300);  // 1 record per page. 300 leaf pages.
    COERCE_ERROR(engine.get_storage_manager().create_array(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    EXPECT_EQ(0, out->get_pimpl()->root_page_pointer_.volatile_pointer_.components.numa_node);
    ArrayPage* root = out->get_pimpl()->root_page_;
    EXPECT_EQ(0, root->get_interior_record(0).volatile_pointer_.components.numa_node);
    EXPECT_EQ(1U, root->get_interior_record(1).volatile_pointer_.components.numa_node);
    const memory::GlobalVolatilePageResolver& resolver
      = engine.get_memory_manager().get_global_volatile_page_resolver();
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

typedef void (*TestFunctor)(ArrayPartitioner* partitioner);
void execute_test(TestFunctor functor, uint64_t array_size = 1024) {
  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = 2;  // otherwise we can't test partitioning
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayStorage* out;
    Epoch commit_epoch;
    ArrayMetadata meta("test", kPayload, array_size);
    COERCE_ERROR(engine.get_storage_manager().create_array(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    ArrayPartitioner* partitioner =
      reinterpret_cast<ArrayPartitioner*>(Partitioner::create_partitioner(&engine, out->get_id()));
    EXPECT_TRUE(partitioner != nullptr);
    functor(partitioner);
    delete partitioner;
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

void CloneFunctor(ArrayPartitioner* partitioner) {
  ArrayPartitioner* cloned = dynamic_cast<ArrayPartitioner*>(partitioner->clone());
  EXPECT_TRUE(cloned != nullptr);
  EXPECT_NE(cloned, partitioner);
  delete cloned;
}

TEST(ArrayPartitionerTest, Clone) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&CloneFunctor);
}

void EmptyFunctor(ArrayPartitioner* partitioner) {
  snapshot::BufferPosition dummy[1];
  snapshot::LogBuffer log_buffer(nullptr);
  PartitionId results[1];
  partitioner->partition_batch(0, log_buffer, dummy, 0, results);
}
TEST(ArrayPartitionerTest, Empty) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&EmptyFunctor);
}

template <int LOG_COUNT>
struct Logs {
  explicit Logs(ArrayPartitioner* partitioner)
  : log_buffer_(memory_), partitioner_(partitioner), cur_pos_(0), cur_count_(0) {
    std::memset(memory_, 0xDA, LOG_COUNT * 64);  // fill with garbage
    sort_buffer_.alloc(LOG_COUNT * 16, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  }

  void add_log(Epoch::EpochInteger epoch_int, uint16_t ordinal,
               ArrayOffset offset, uint16_t payload_offset, uint16_t payload_count) {
    ArrayOverwriteLogType* entry = reinterpret_cast<ArrayOverwriteLogType*>(memory_ + cur_pos_);
    char dummy[kPayload];
    entry->populate(partitioner_->get_storage_id(), offset, dummy, payload_offset, payload_count);
    entry->header_.xct_id_.set(epoch_int, ordinal);
    positions_[cur_count_] = log_buffer_.compact(entry);
    cur_pos_ += entry->header_.log_length_;
    ++cur_count_;
  }

  void partition_batch() {
    partitioner_->partition_batch(
      0,
      log_buffer_,
      positions_,
      cur_count_,
      partition_results_);
  }
  uint32_t sort_batch(Epoch::EpochInteger base_epoch) {
    uint32_t written_count = 0;
    partitioner_->sort_batch(
      log_buffer_,
      positions_,
      cur_count_,
      memory::AlignedMemorySlice(&sort_buffer_),
      Epoch(base_epoch),
      sort_results_,
      &written_count);
    return written_count;
  }

  char   memory_[LOG_COUNT * 64];
  snapshot::LogBuffer log_buffer_;
  ArrayPartitioner*   partitioner_;
  uint64_t cur_pos_;
  uint32_t cur_count_;
  snapshot::BufferPosition positions_[LOG_COUNT];
  PartitionId partition_results_[LOG_COUNT];
  snapshot::BufferPosition sort_results_[LOG_COUNT];
  memory::AlignedMemory sort_buffer_;
};

void PartitionBasicFunctor(ArrayPartitioner* partitioner) {
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


void SortBasicFunctor(ArrayPartitioner* partitioner) {
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

void SortCompactFunctor(ArrayPartitioner* partitioner) {
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

void SortNoCompactFunctor(ArrayPartitioner* partitioner) {
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
