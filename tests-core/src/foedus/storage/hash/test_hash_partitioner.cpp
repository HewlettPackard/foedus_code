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
#include <set>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_partitioner_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {
DEFINE_TEST_CASE_PACKAGE(HashPartitionerTest, foedus.storage.hash);

const char* kTableName = "test";
const uint8_t kBinBits = 10U;

std::set<HashBin> get_non_empty_bins(uint32_t records) {
  std::set<HashBin> ret;
  for (uint64_t i = 0; i < records; ++i) {
    HashBin bin = hashinate(i) >> (64 - kBinBits);
    ret.insert(bin);
  }
  return ret;
}

/**
 * Populates the test table with records.
 * Records with even bin are done in node-0, odd bin are done in node-1.
 * So, this procedure is called twice.
 */
ErrorStack populate_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t records = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  HashStorage storage(context->get_engine(), kTableName);
  EXPECT_TRUE(storage.exists());
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  bool even_only = context->get_thread_id() == 0;

  std::set<HashBin> bins;
  for (uint64_t i = 0; i < records; ++i) {
    HashCombo combo = storage.combo(&i);
    bins.insert(combo.bin_);
    uint32_t data = i;
    if ((even_only && (combo.bin_ % 2U) == 0)
      || (!even_only && (combo.bin_ % 2U) == 1U)) {
      WRAP_ERROR_CODE(storage.insert_record(context, &i, sizeof(i), &data, sizeof(data)));
    }
  }
  std::set<HashBin> bins_another = get_non_empty_bins(records);
  EXPECT_EQ(bins, bins_another);
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}


typedef void (*TestFunctor)(Partitioner partitioner, uint32_t records);
void execute_test(TestFunctor functor, uint32_t records) {
  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = 2;  // otherwise we can't test partitioning
  options.thread_.thread_count_per_group_ = 1;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("populate_task", populate_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashStorage out;
    Epoch commit_epoch;
    HashMetadata meta(kTableName, kBinBits);
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    for (uint16_t node = 0; node < 2U; ++node) {
      COERCE_ERROR(engine.get_thread_pool()->impersonate_on_numa_node_synchronous(
        node,
        "populate_task",
        &records,
        sizeof(records)));
    }

    HashStorage storage(&engine, kTableName);
    // TASK(Hideaki) verify API for hash
    // COERCE_ERROR(storage.verify_single_thread(context));
    // only when debugging
    // COERCE_ERROR(storage.debugout_single_thread(&engine, true, true));

    Partitioner partitioner(&engine, out.get_id());
    memory::AlignedMemory work_memory;
    work_memory.alloc(1U << 21, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
    cache::SnapshotFileSet fileset(&engine);
    COERCE_ERROR(fileset.initialize())
    Partitioner::DesignPartitionArguments args = { &work_memory, &fileset};
    COERCE_ERROR(partitioner.design_partition(args));

    // whitebox-begin: this part is a whitebox test. yes, pros and cons to do that.
    // but, it's helpful for hash where we can't easily generate a key with arbitrary hash.
    // let's directly examine all bins in HashPartitionerData.
    HashPartitionerData* partitioner_data = reinterpret_cast<HashPartitionerData*>(
      PartitionerMetadata::get_metadata(&engine, meta.id_)->locate_data(&engine));
    EXPECT_EQ(1U << kBinBits, partitioner_data->total_bin_count_);
    EXPECT_TRUE(partitioner_data->partitionable_);
    EXPECT_EQ(kBinBits, partitioner_data->bin_bits_);
    EXPECT_EQ(64 - kBinBits, partitioner_data->bin_shifts_);
    std::set<HashBin> non_empty = get_non_empty_bins(records);
    for (HashBin bin = 0; bin< (1U << kBinBits); ++bin) {
      if (non_empty.find(bin) == non_empty.end()) {
        EXPECT_EQ(0, partitioner_data->bin_owners_[bin]) << bin;
      } else if (bin % 2U == 0) {
        EXPECT_EQ(0, partitioner_data->bin_owners_[bin]) << bin;
      } else {
        EXPECT_EQ(1U, partitioner_data->bin_owners_[bin]) << bin;
      }
    }
    // whitebox-end

    COERCE_ERROR(fileset.uninitialize());
    EXPECT_TRUE(partitioner.is_valid());
    functor(partitioner, records);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

void EmptyFunctor(Partitioner partitioner, uint32_t /*records*/) {
  snapshot::BufferPosition dummy[1];
  snapshot::LogBuffer log_buffer(nullptr);
  PartitionId results[1];
  Partitioner::PartitionBatchArguments args = { 0, log_buffer, dummy, 0, results };
  partitioner.partition_batch(args);
}

template <int LOG_COUNT>
struct Logs {
  explicit Logs(Partitioner partitioner)
  : log_buffer_(memory_), partitioner_(partitioner), cur_pos_(0), cur_count_(0) {
    std::memset(memory_, 0xDA, sizeof(memory_));  // fill with garbage
    sort_buffer_.alloc(LOG_COUNT * 16, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  }

  void add_log(Epoch::EpochInteger epoch_int, uint32_t ordinal, uint64_t key) {
    HashInsertLogType* entry = reinterpret_cast<HashInsertLogType*>(memory_ + cur_pos_);
    uint32_t data = key;
    entry->populate(partitioner_.get_storage_id(), &key, sizeof(key), &data, sizeof(data));
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
      sizeof(uint64_t),
      sizeof(uint64_t),
      &sort_buffer_,
      Epoch(base_epoch),
      sort_results_,
      &written_count};
    partitioner_.sort_batch(args);
    return written_count;
  }
  HashInsertLogType* resolve(snapshot::BufferPosition position) {
    return reinterpret_cast<HashInsertLogType*>(log_buffer_.resolve(position));
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

const uint32_t kTests = 128U;

void PartitionBasicFunctor(Partitioner partitioner, uint32_t records) {
  std::unique_ptr< Logs<kTests> > logs(new Logs<kTests>(partitioner));
  for (uint32_t i = 0; i < kTests; ++i) {
    logs->add_log(2, i + 1, i * 16);
  }
  logs->partition_batch();
  std::set<HashBin> non_empty = get_non_empty_bins(records);
  for (uint32_t i = 0; i < kTests; ++i) {
    uint64_t key = i * 16;
    HashBin bin = hashinate(key) >> (64 - kBinBits);
    if (non_empty.find(bin) == non_empty.end()) {
      // so far, bins that didn't receive any record will be partitioned to node-0.
      EXPECT_EQ(0, logs->partition_results_[i]) << i << "," << bin;
    } else if (bin % 2 == 0) {
      EXPECT_EQ(0, logs->partition_results_[i]) << i << "," << bin;
    } else {
      EXPECT_EQ(1U, logs->partition_results_[i]) << i << "," << bin;
    }
  }
}

void SortBasicFunctor(Partitioner partitioner, uint32_t /*records*/) {
  std::unique_ptr< Logs<kTests> > logs(new Logs<kTests>(partitioner));
  for (uint32_t i = 0; i < kTests; ++i) {
    logs->add_log(2, i + 1, i * 16);
  }
  EXPECT_EQ(kTests, logs->sort_batch(2));
  for (uint32_t i = 1U; i < kTests; ++i) {
    ASSERT_LT(logs->sort_results_[i - 1] * 8U, logs->cur_pos_);
    ASSERT_LT(logs->sort_results_[i] * 8U, logs->cur_pos_);
    HashInsertLogType* pre = logs->resolve(logs->sort_results_[i - 1]);
    HashInsertLogType* cur = logs->resolve(logs->sort_results_[i]);

    EXPECT_EQ(sizeof(uint64_t), pre->key_length_);
    EXPECT_EQ(sizeof(uint64_t), cur->key_length_);
    uint64_t cur_key = *reinterpret_cast<uint64_t*>(cur->get_key());
    uint64_t pre_key = *reinterpret_cast<uint64_t*>(pre->get_key());
    EXPECT_EQ(0, cur_key % 16U);
    EXPECT_EQ(0, pre_key % 16U);
    EXPECT_LT(cur_key, kTests * 16U);
    EXPECT_LT(pre_key, kTests * 16U);
    HashBin cur_bin = hashinate(cur->get_key(), cur->key_length_) >> (64 - kBinBits);
    HashBin pre_bin = hashinate(pre->get_key(), pre->key_length_) >> (64 - kBinBits);

    EXPECT_EQ(2U, pre->header_.xct_id_.get_epoch_int());
    EXPECT_EQ(2U, cur->header_.xct_id_.get_epoch_int());
    uint32_t cur_ordinal = cur->header_.xct_id_.get_ordinal();
    uint32_t pre_ordinal = pre->header_.xct_id_.get_ordinal();

    // results should be ordered by bins
    EXPECT_TRUE(pre_bin < cur_bin
      || (pre_bin == cur_bin && pre_ordinal <= cur_ordinal)) << i;
  }
}
TEST(HashPartitionerTest, Empty) { execute_test(&EmptyFunctor, 16); }
TEST(HashPartitionerTest, EmptyMany) { execute_test(&EmptyFunctor, 1024); }

TEST(HashPartitionerTest, PartitionBasic) { execute_test(&PartitionBasicFunctor, 16); }
TEST(HashPartitionerTest, PartitionBasicMany) { execute_test(&PartitionBasicFunctor, 1024); }

// sorting has nothing with partitioning, so no need for "many" training inputs.
TEST(HashPartitionerTest, SortBasic) { execute_test(&SortBasicFunctor, 16); }

}  // namespace hash
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashPartitionerTest, foedus.storage.hash);
