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
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreePartitionerTest, foedus.storage.masstree);

const char* kTableName = "test";
KeySlice nm(uint64_t key) { return normalize_primitive<uint64_t>(key); }

/**
 * Populates the test table with records.
 * First half is done in node-0, second half is done in node-1.
 * So, this procedure is called twice.
 */
ErrorStack populate_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t table_size = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  MasstreeStorage storage(context->get_engine(), kTableName);
  EXPECT_TRUE(storage.exists());
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;
  COERCE_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint64_t i = 0; i < table_size / 2U; ++i) {
    uint32_t id = i + (context->get_thread_id() == 0 ? 0 : table_size / 2U);
    COERCE_ERROR(storage.insert_record_normalized(context, nm(id), &id, sizeof(id)));
  }
  COERCE_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  COERCE_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  COERCE_ERROR(storage.verify_single_thread(context));
  COERCE_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  COERCE_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}


typedef void (*TestFunctor)(Partitioner partitioner);
void execute_test(TestFunctor functor, uint32_t table_size = 1024) {
  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = 2;  // otherwise we can't test partitioning
  options.thread_.thread_count_per_group_ = 1;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("populate_task", populate_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage out;
    Epoch commit_epoch;
    MasstreeMetadata meta(kTableName);
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    for (uint16_t node = 0; node < 2U; ++node) {
      COERCE_ERROR(engine.get_thread_pool()->impersonate_on_numa_node_synchronous(
        node,
        "populate_task",
        &table_size,
        sizeof(table_size)));
    }
    Partitioner partitioner(&engine, out.get_id());
    memory::AlignedMemory work_memory;
    work_memory.alloc(1U << 21, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
    cache::SnapshotFileSet fileset(&engine);
    COERCE_ERROR(fileset.initialize())
    Partitioner::DesignPartitionArguments args = {
      memory::AlignedMemorySlice(&work_memory),
      &fileset};
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
TEST(MasstreePartitionerTest, Empty) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&EmptyFunctor, 1024);
}

template <int LOG_COUNT>
struct Logs {
  explicit Logs(Partitioner partitioner)
  : log_buffer_(memory_), partitioner_(partitioner), cur_pos_(0), cur_count_(0) {
    std::memset(memory_, 0xDA, sizeof(memory_));  // fill with garbage
    sort_buffer_.alloc(LOG_COUNT * 16, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  }

  void add_log(Epoch::EpochInteger epoch_int, uint32_t ordinal, uint32_t key) {
    MasstreeInsertLogType* entry = reinterpret_cast<MasstreeInsertLogType*>(memory_ + cur_pos_);
    char key_be[sizeof(KeySlice)];
    assorted::write_bigendian<KeySlice>(nm(key), key_be);
    uint32_t data = key;
    entry->populate(partitioner_.get_storage_id(), key_be, sizeof(key_be), &data, sizeof(data), 0);
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
      memory::AlignedMemorySlice(&sort_buffer_),
      Epoch(base_epoch),
      sort_results_,
      &written_count};
    partitioner_.sort_batch(args);
    return written_count;
  }

  char   memory_[LOG_COUNT * 128];
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
  std::unique_ptr< Logs<64> > logs(new Logs<64>(partitioner));
  for (int i = 0; i < 64; ++i) {
    logs->add_log(2, i + 1, i * 16);
  }
  logs->partition_batch();
  for (int i = 0; i < 28; ++i) {  // maybe the partition design is a bit inaccurate, so not 32
    EXPECT_EQ(0, logs->partition_results_[i]) << i;
  }
  for (int i = 36; i < 64; ++i) {  // same above
    EXPECT_EQ(1, logs->partition_results_[i]) << i;
  }
}

TEST(MasstreePartitionerTest, PartitionBasic) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&PartitionBasicFunctor);
}


void SortBasicFunctor(Partitioner partitioner) {
  std::unique_ptr< Logs<64> > logs(new Logs<64>(partitioner));
  for (int i = 0; i < 64; ++i) {
    logs->add_log(2, i + 1, 1024 - i * 16);
  }
  EXPECT_EQ(64, logs->sort_batch(2));
  for (int i = 0; i < 64; ++i) {
    // results should be reverse-ordered
    EXPECT_EQ(logs->positions_[63 - i], logs->sort_results_[i]) << i;
  }
}

TEST(MasstreePartitionerTest, SortBasic) {
  if (!is_multi_nodes()) {
    return;
  }
  execute_test(&SortBasicFunctor);
}
}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreePartitionerTest, foedus.storage.masstree);
