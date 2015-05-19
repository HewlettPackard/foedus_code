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
// #include <gperftools/profiler.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <set>
#include <string>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/log_buffer.hpp"
#include "foedus/snapshot/merge_sort.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"

/**
 * @file test_merge_sort.cpp
 * Testcase for MergeSort.
 */
namespace foedus {
namespace snapshot {
DEFINE_TEST_CASE_PACKAGE(MergeSortTest, foedus.snapshot);

const storage::StorageId kStorageId = 1;
const uint16_t kChunkBatch = 1 << 5;  // less to make the test faster
const uint64_t kLogsPerInput = 1 << 16;
const uint32_t kPayload = 8;
const uint32_t kFetchSize = 4;

enum TestKeyDistribution {
  /** logs have distinct keys */
  kDistinctKey,
  /** logs might have duplicate keys, but can be differentiated with epoch */
  kDistinctEpoch,
  /** logs might have duplicate keys and epoch, but can be differentiated with ordinal */
  kDistinctOrdinal,
  /** logs might have full duplicate, internally differentiated by buffer position. */
  kDuplicates,
};

std::string to_string(TestKeyDistribution distribution) {
  switch (distribution) {
  case kDistinctKey: return "kDistinctKey";
  case kDistinctEpoch: return "kDistinctEpoch";
  case kDistinctOrdinal: return "kDistinctOrdinal";
  default:
    ASSERT_ND(distribution == kDuplicates);
    return "kDuplicates";
  }
}

uint64_t to_key(uint64_t i, TestKeyDistribution distribution) {
  switch (distribution) {
  case kDistinctKey: return i;
  case kDistinctEpoch: return i / 16;
  case kDistinctOrdinal: return i / 256;
  default:
    ASSERT_ND(distribution == kDuplicates);
    return i / 512;
  }
}

Epoch::EpochInteger to_epoch(uint64_t i, TestKeyDistribution distribution) {
  switch (distribution) {
  case kDistinctKey: return 1;
  case kDistinctEpoch: return 1 + (i % 16);
  case kDistinctOrdinal: return 1 + ((i % 256) / 16);
  default:
    ASSERT_ND(distribution == kDuplicates);
    return 1 + ((i % 512) / 32);
  }
}
uint32_t to_in_epoch_ordinal(uint64_t i, TestKeyDistribution distribution) {
  switch (distribution) {
  case kDistinctKey: return 1;
  case kDistinctEpoch: return 1;
  case kDistinctOrdinal: return 1 + (i % 16);
  default:
    ASSERT_ND(distribution == kDuplicates);
    return 1 + ((i % 32) / 2);
  }
}

/**
 * base test implementation. derived classes below to implement 1-input/2-inputs testcases.
 */
struct TestBase {
  TestBase(
    TestKeyDistribution distribution,
    uint32_t shortest_key_length,
    uint32_t longest_key_length,
    uint64_t max_log_length)
    : distribution_(distribution),
      shortest_key_length_(shortest_key_length),
      longest_key_length_(longest_key_length),
      max_log_length_(max_log_length),
      inmemory_buffer_(nullptr) {
    ASSERT_ND(shortest_key_length_ <= longest_key_length_);
    capacity_ = max_log_length * kLogsPerInput;
    memory_.alloc(capacity_, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
  }

  virtual ~TestBase() {
    delete inmemory_buffer_;
    inmemory_buffer_ = nullptr;
  }

  template <typename POPULATE>
  uint64_t invoke_populate(uint64_t i, uint64_t cur, char* buf, char* payload, POPULATE populate) {
    auto* entry = reinterpret_cast<log::RecordLogType*>(buf + cur);
    std::memcpy(payload, &i, sizeof(uint64_t));
    populate(entry, to_key(i, distribution_), payload);
    entry->header_.xct_id_.set(to_epoch(i, distribution_), to_in_epoch_ordinal(i, distribution_));
    cur += entry->header_.log_length_;
    ASSERT_ND(entry->header_.log_length_ > 0);
    ASSERT_ND(entry->header_.log_length_ <= max_log_length_);
    ASSERT_ND(cur <= capacity_);
    return cur;
  }

  virtual uint16_t get_input_count() const = 0;
  virtual SortedBuffer** get_inputs() = 0;

  template <typename VERIFY>
  void merge_inputs(
    storage::StorageType storage_type,
    VERIFY verify) {
    memory::AlignedMemory work_memory;
    work_memory.alloc(1U << 21, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);

    MergeSort merge_sort(
      kStorageId,
      storage_type,
      Epoch(1),
      get_inputs(),
      get_input_count(),
      32,
      &work_memory,
      kChunkBatch);
    COERCE_ERROR(merge_sort.initialize());
    uint64_t total_processed = 0;
    while (true) {
      COERCE_ERROR(merge_sort.next_batch());
      if (merge_sort.get_current_count() == 0 && merge_sort.is_ended_all()) {
        break;
      }
      for (uint64_t i = 0; i < merge_sort.get_current_count(); ++i) {
        uint64_t accumulative_i = i + total_processed;
        uint64_t key = to_key(accumulative_i, distribution_);
        uint16_t epoch = to_epoch(accumulative_i, distribution_);
        uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution_);
        SCOPED_TRACE(testing::Message() << "i=" << accumulative_i << ", key=" << key
            << ", epoch=" << epoch << ", ordinal=" << ordinal);

        uint32_t position = merge_sort.get_sort_entries()[i].get_position();
        ASSERT_ND(position < merge_sort.get_current_count());

        const MergeSort::PositionEntry& pos = merge_sort.get_position_entries()[position];
        if (get_input_count() == 1U) {
          // this test is valid because this is a single-input testcase without window-shift.
          EXPECT_EQ(merge_sort.resolve_sort_position(i), merge_sort.resolve_merged_position(i));
          EXPECT_EQ(0, pos.input_index_);
        } else {
          EXPECT_EQ(accumulative_i % get_input_count(), pos.input_index_);
        }

        if (storage_type == storage::kArrayStorage) {
          EXPECT_TRUE(pos.get_log_type() == log::kLogCodeArrayOverwrite
            || pos.get_log_type() == log::kLogCodeArrayIncrement);
        } else if (storage_type == storage::kHashStorage) {
          EXPECT_TRUE(pos.get_log_type() == log::kLogCodeHashInsert
            || pos.get_log_type() == log::kLogCodeHashDelete
            || pos.get_log_type() == log::kLogCodeHashOverwrite);
        } else {
          EXPECT_TRUE(pos.get_log_type() == log::kLogCodeMasstreeInsert
            || pos.get_log_type() == log::kLogCodeMasstreeDelete
            || pos.get_log_type() == log::kLogCodeMasstreeOverwrite);
        }

        const auto* log = reinterpret_cast<const log::RecordLogType*>(
          merge_sort.resolve_sort_position(i));
        EXPECT_EQ(epoch, log->header_.xct_id_.get_epoch_int());
        EXPECT_EQ(ordinal, log->header_.xct_id_.get_ordinal());
        verify(log, key, reinterpret_cast<const char*>(&accumulative_i));
      }

      // also try fetch_logs
      const storage::array::ArrayOverwriteLogType* logs[kFetchSize];
      for (uint64_t i = 0; i < merge_sort.get_current_count();) {
        uint32_t fetched = merge_sort.fetch_logs(
          i,
          kFetchSize,
          reinterpret_cast<log::RecordLogType const**>(logs));
        for (uint64_t j = 0; j < fetched; ++j) {
          uint64_t accumulative_i = i + j + total_processed;
          uint64_t key = to_key(accumulative_i, distribution_);
          uint16_t epoch = to_epoch(accumulative_i, distribution_);
          uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution_);
          SCOPED_TRACE(testing::Message() << "i=" << accumulative_i << ", key=" << key
              << ", epoch=" << epoch << ", ordinal=" << ordinal);
          EXPECT_EQ(epoch, logs[j]->header_.xct_id_.get_epoch_int());
          EXPECT_EQ(ordinal, logs[j]->header_.xct_id_.get_ordinal());
          verify(logs[j], key, reinterpret_cast<const char*>(&accumulative_i));
        }
        i+= fetched;
        ASSERT_ND(i <= merge_sort.get_current_count());
      }

      // also also, try groupify.
      // because these testcases use just one log type, this only covers the common-key cases.
      // better to have a specific testcase to cover common log-type cases.
      uint64_t cur;
      for (cur = 0; cur < merge_sort.get_current_count();) {
        MergeSort::GroupifyResult result = merge_sort.groupify(cur);
        ASSERT_ND(result.count_ > 0);
        if (result.count_ == 1U) {
          EXPECT_FALSE(result.has_common_key_);
          EXPECT_FALSE(result.has_common_log_code_);
          if (cur + 1U < merge_sort.get_current_count()) {
            uint64_t cur_key = to_key(cur + total_processed, distribution_);
            uint64_t next_key = to_key(cur + 1U + total_processed, distribution_);
            EXPECT_NE(cur_key, next_key) << cur;
          }
        } else if (result.has_common_key_) {
          EXPECT_FALSE(result.has_common_log_code_);
          ASSERT_ND(result.count_ > 1U);
          for (uint32_t i = 0; i < result.count_ - 1U; ++i) {
            uint64_t cur_key = to_key(cur + i + total_processed, distribution_);
            uint64_t next_key = to_key(cur + i + 1U + total_processed, distribution_);
            EXPECT_EQ(cur_key, next_key) << cur;
          }
          if (cur + result.count_ < merge_sort.get_current_count()) {
            uint64_t cur_key = to_key(cur + result.count_ - 1U + total_processed, distribution_);
            uint64_t next_key = to_key(cur + result.count_ + total_processed, distribution_);
            EXPECT_NE(cur_key, next_key) << cur;
          }
        } else {
          EXPECT_TRUE(result.has_common_log_code_);
          ASSERT_ND(result.count_ > 1U);
        }

        // std::cout << "group " << cur << "-" << (cur + result.count_)
        //   << ". common-key=" << result.has_common_key_
        //   << ", common_type=" << result.has_common_log_code_ << std::endl;
        cur += result.count_;
      }
      ASSERT_ND(cur == merge_sort.get_current_count());

      total_processed += merge_sort.get_current_count();
    }
    COERCE_ERROR(merge_sort.uninitialize());
    EXPECT_EQ(kLogsPerInput * get_input_count(), total_processed);
  }

  TestKeyDistribution distribution_;
  uint64_t capacity_;
  uint32_t shortest_key_length_;
  uint32_t longest_key_length_;
  uint64_t max_log_length_;

  // in-memory buffer (1st buffer)
  memory::AlignedMemory memory_;
  InMemorySortedBuffer* inmemory_buffer_;
};

/**
 * testcase for single input (in-memory buffer only). obviously so simple.
 */
struct SingleInputTest : public TestBase {
  SingleInputTest(
    TestKeyDistribution distribution,
    uint32_t shortest_key_length,
    uint32_t longest_key_length,
    uint64_t max_log_length)
    : TestBase(distribution, shortest_key_length, longest_key_length, max_log_length) {
  }

  ~SingleInputTest() {}

  template <typename POPULATE>
  void prepare_inputs(POPULATE populate) {
    uint64_t cur = 0;
    char* buf = reinterpret_cast<char*>(memory_.get_block());
    char payload[kPayload];
    std::memset(payload, 0, kPayload);
    for (uint64_t i = 0; i < kLogsPerInput; ++i) {
      cur = invoke_populate(i, cur, buf, payload, populate);
    }

    inmemory_buffer_ = new InMemorySortedBuffer(buf, cur);
    inmemory_buffer_->set_current_block(
      kStorageId,
      kLogsPerInput,
      0,
      cur,
      shortest_key_length_,
      longest_key_length_);
  }
  uint16_t get_input_count() const override { return 1; }
  SortedBuffer** get_inputs() override {
    inputs_[0] = inmemory_buffer_;
    return inputs_;
  }

  SortedBuffer* inputs_[1];
};

/**
 * testcase for multiple inputs (1 in-memory buffer and 1 on-disk buffer).
 * in-memory buffer takes even-numbered keys and on-disk buffer takes odd-numbered keys.
 */
struct MultiInputsTest : public TestBase {
  MultiInputsTest(
    const std::string& experiment_name,
    TestKeyDistribution distribution,
    uint32_t shortest_key_length,
    uint32_t longest_key_length,
    uint64_t max_log_length)
    : TestBase(distribution, shortest_key_length, longest_key_length, max_log_length),
      path_(get_random_tmp_file_path(experiment_name + to_string(distribution))),
      file_(path_),
      io_buffer_(nullptr) {
    io_memory_.alloc(capacity_, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
  }

  ~MultiInputsTest() {
    delete io_buffer_;
    io_buffer_ = nullptr;
    EXPECT_TRUE(file_.close());
    fs::remove_all(path_.parent_path());
  }

  template <typename POPULATE>
  void prepare_inputs(POPULATE populate) {
    uint64_t cur = 0;
    char* buf = reinterpret_cast<char*>(memory_.get_block());
    char payload[kPayload];
    std::memset(payload, 0, kPayload);
    for (uint64_t i = 0; i < kLogsPerInput * 2U; i += 2) {  // even numbers
      cur = invoke_populate(i, cur, buf, payload, populate);
    }

    inmemory_buffer_ = new InMemorySortedBuffer(buf, cur);
    inmemory_buffer_->set_current_block(
      kStorageId,
      kLogsPerInput,
      0,
      cur,
      shortest_key_length_,
      longest_key_length_);

    cur = 0;
    buf = reinterpret_cast<char*>(io_memory_.get_block());
    for (uint64_t i = 1; i < kLogsPerInput * 2U; i += 2) {  // odd numbers
      cur = invoke_populate(i, cur, buf, payload, populate);
    }

    EXPECT_TRUE(fs::create_directories(path_.parent_path()));
    COERCE_ERROR_CODE(file_.open(true, true, true, true));
    COERCE_ERROR_CODE(file_.write(capacity_, io_memory_));
    COERCE_ERROR_CODE(file_.sync());

    // open it again to read from the beginning
    EXPECT_TRUE(file_.close());
    COERCE_ERROR_CODE(file_.open(true, false, false, false));

    // smaller buffer size on purpose. causes window move
    io_window_.alloc(capacity_ / 6U, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
    memory::AlignedMemorySlice slice(&io_window_);
    COERCE_ERROR_CODE(file_.read(slice.get_size(), slice));

    io_buffer_ = new DumpFileSortedBuffer(&file_, slice);
    io_buffer_->set_current_block(
      kStorageId,
      kLogsPerInput,
      0,
      cur,
      shortest_key_length_,
      longest_key_length_);
  }

  uint16_t get_input_count() const override { return 2; }
  SortedBuffer** get_inputs() override {
    inputs_[0] = inmemory_buffer_;
    inputs_[1] = io_buffer_;
    return inputs_;
  }

  SortedBuffer* inputs_[2];
  fs::Path path_;
  fs::DirectIoFile file_;

  // on-disk buffer (2nd buffer)
  memory::AlignedMemory io_memory_;
  memory::AlignedMemory io_window_;
  DumpFileSortedBuffer* io_buffer_;
};

///////////////////////////////////////////////////
/// Array testcases
///////////////////////////////////////////////////
void array_populate(log::RecordLogType* log, uint64_t key, const char* payload) {
  auto* entry = reinterpret_cast<storage::array::ArrayOverwriteLogType*>(log);
  entry->populate(kStorageId, key, payload, 0, kPayload);
}
void array_verify(const log::RecordLogType* log, uint64_t key, const char* payload) {
  const auto* entry = reinterpret_cast<const storage::array::ArrayOverwriteLogType*>(log);
  EXPECT_EQ(key, entry->offset_);
  EXPECT_EQ(0, entry->payload_offset_);
  EXPECT_EQ(kPayload, entry->payload_count_);
  EXPECT_EQ(0, std::memcmp(entry->payload_, payload, kPayload));
}

void test_single_inputs_array(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(storage::array::ArrayOffset);
  uint16_t length = storage::array::ArrayOverwriteLogType::calculate_log_length(kPayload);
  SingleInputTest impl(distribution, kLen, kLen, length);
  impl.prepare_inputs(array_populate);
  impl.merge_inputs(storage::kArrayStorage, array_verify);
}

void test_multi_inputs_array(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(storage::array::ArrayOffset);
  uint16_t length = storage::array::ArrayOverwriteLogType::calculate_log_length(kPayload);
  MultiInputsTest impl("test_multi_inputs_array_", distribution, kLen, kLen, length);
  impl.prepare_inputs(array_populate);
  impl.merge_inputs(storage::kArrayStorage, array_verify);
}

///////////////////////////////////////////////////
/// Hash testcases
/// Here, "key" for merge-sort must have unique hashbin. Further,
/// individual inputs must be ordered by that, which was trivial in array/masstree.
/// We thus pre-generate such keys.
///////////////////////////////////////////////////

struct OrderedHashKey {
  storage::hash::HashBin  bin_;
  /** the key that is that is ideally unique and ordered */
  uint64_t                key_;
  /** this is just for debugging. not used. */
  uint64_t                original_key_;

  bool operator<(const OrderedHashKey& other) const { return bin_ < other.bin_; }
};

const uint16_t  kBufSize = 16;
const uint8_t   kHashBinBits = 31;  // big enough to avoid hash collisions
const uint32_t  kKeyCount = kLogsPerInput * 2U;

bool                        g_hash_pregenerated = false;
std::vector<OrderedHashKey> g_hash_ordered_fixlen;
std::vector<OrderedHashKey> g_hash_ordered_varlen;

/** Simply "keystr<number>" */
uint16_t construct_varlen_hash(uint64_t int_key, char* key) {
  // this puts an unnecessary null char at the end, but doesn't matter.
  std::snprintf(key, kBufSize, "keystr%ju", int_key);
  return std::strlen(key);
}


void hash_pregenerate_if_needed() {
  if (LIKELY(g_hash_pregenerated)) {
    return;
  }
  g_hash_ordered_fixlen.clear();
  g_hash_ordered_varlen.clear();
  LOG(INFO) << "Pre-generating hash keys...";

  std::set<OrderedHashKey> set_fixlen;
  std::set<OrderedHashKey> set_varlen;
  char buf[kBufSize];
  for (uint64_t key = 0; key < kKeyCount; ++key) {
    uint64_t cur = key;
    while (true) {
      storage::hash::HashBin fixlen_bin
        = storage::hash::hashinate(&cur, sizeof(cur)) >> (64U - kHashBinBits);
      OrderedHashKey fixlen_entry = {fixlen_bin, cur, key};
      if (set_fixlen.find(fixlen_entry) != set_fixlen.end()) {
        LOG(INFO) << "Ouuuuuuuuuuu. hash collision in fixlen key. we must reroll:"
          << cur << ", bin=" << fixlen_bin;
        cur += kKeyCount;  // reroll
        continue;
      }

      uint16_t varlen = construct_varlen_hash(cur, buf);
      storage::hash::HashBin varlen_bin
        = storage::hash::hashinate(buf, varlen) >> (64U - kHashBinBits);
      OrderedHashKey varlen_entry = {varlen_bin, cur, key};
      if (set_varlen.find(varlen_entry) != set_varlen.end()) {
        LOG(INFO) << "Ouuuuuuuuuuu. hash collision in varlen key. we must reroll:"
          << cur << ", bin=" << varlen_bin;
        cur += kKeyCount;  // reroll
        continue;
      }

      set_fixlen.insert(fixlen_entry);
      set_varlen.insert(varlen_entry);
      break;
    }
  }

  g_hash_ordered_fixlen.assign(set_fixlen.cbegin(), set_fixlen.cend());
  g_hash_ordered_varlen.assign(set_varlen.cbegin(), set_varlen.cend());
  ASSERT_ND(g_hash_ordered_fixlen.size() == kKeyCount);
  ASSERT_ND(g_hash_ordered_varlen.size() == kKeyCount);

  LOG(INFO) << "Pre-generated hash keys.";
  ASSERT_ND(g_hash_pregenerated == false);
  g_hash_pregenerated = true;
}

/**
 * @param[in] int_key the key generated by to_key().
 * @return the actual key value we should use.
 */
uint64_t to_hash_safe_fixlen_key(uint64_t int_key) {
  ASSERT_ND(int_key < kKeyCount);
  hash_pregenerate_if_needed();
  return g_hash_ordered_fixlen[int_key].key_;
}
uint16_t to_hash_safe_varlen_key(uint64_t int_key, char* key) {
  ASSERT_ND(int_key < kKeyCount);
  hash_pregenerate_if_needed();
  uint64_t ordered_key = g_hash_ordered_varlen[int_key].key_;
  return construct_varlen_hash(ordered_key, key);
}

void hash_fixlen_populate(log::RecordLogType* log, uint64_t int_key, const char* payload) {
  uint64_t key = to_hash_safe_fixlen_key(int_key);
  auto* entry = reinterpret_cast<storage::hash::HashInsertLogType*>(log);
  storage::hash::HashValue hash = storage::hash::hashinate(&key, sizeof(key));
  entry->populate(kStorageId, &key, sizeof(key), kHashBinBits, hash, payload, kPayload);
}
void hash_fixlen_verify(const log::RecordLogType* log, uint64_t int_key, const char* payload) {
  uint64_t key = to_hash_safe_fixlen_key(int_key);
  const auto* entry = reinterpret_cast<const storage::hash::HashInsertLogType*>(log);
  EXPECT_EQ(kHashBinBits, entry->bin_bits_);
  EXPECT_EQ(entry->hash_, storage::hash::hashinate(entry->get_key(), entry->key_length_));
  EXPECT_EQ(sizeof(key), entry->key_length_);
  EXPECT_EQ(0, std::memcmp(entry->get_key(), &key, sizeof(key)));
  EXPECT_EQ(kPayload, entry->payload_count_);
  EXPECT_EQ(0, std::memcmp(entry->get_payload(), payload, kPayload));
}

void test_single_input_hash_fixlen(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(uint64_t);
  uint16_t length = storage::hash::HashInsertLogType::calculate_log_length(kLen, kPayload);
  SingleInputTest impl(distribution, kLen, kLen, length);
  impl.prepare_inputs(hash_fixlen_populate);
  impl.merge_inputs(storage::kHashStorage, hash_fixlen_verify);
}

void test_multi_inputs_hash_fixlen(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(uint64_t);
  uint16_t length = storage::hash::HashInsertLogType::calculate_log_length(kLen, kPayload);
  MultiInputsTest impl("test_multi_inputs_hash_fixlen_", distribution, kLen, kLen, length);
  impl.prepare_inputs(hash_fixlen_populate);
  impl.merge_inputs(storage::kHashStorage, hash_fixlen_verify);
}

const uint16_t kVarlenMinHash = 6 + 1;
const uint16_t kVarlenMaxHash = 6 + 9;
const uint16_t kVarlenMaxLogHash
    = storage::hash::HashInsertLogType::calculate_log_length(kVarlenMaxHash, kPayload);

void hash_varlen_populate(log::RecordLogType* log, uint64_t int_key, const char* payload) {
  char key[16];
  uint16_t len = to_hash_safe_varlen_key(int_key, key);
  storage::hash::HashValue hash = storage::hash::hashinate(key, len);
  auto* entry = reinterpret_cast<storage::hash::HashInsertLogType*>(log);
  entry->populate(kStorageId, key, len, kHashBinBits, hash, payload, kPayload);
}
void hash_varlen_verify(const log::RecordLogType* log, uint64_t int_key, const char* payload) {
  char key[16];
  uint16_t len = to_hash_safe_varlen_key(int_key, key);
  const auto* entry = reinterpret_cast<const storage::hash::HashInsertLogType*>(log);
  EXPECT_EQ(len, entry->key_length_);
  EXPECT_EQ(0, std::memcmp(entry->get_key(), key, len));
  EXPECT_EQ(kPayload, entry->payload_count_);
  EXPECT_EQ(0, std::memcmp(entry->get_payload(), payload, kPayload));
}

void test_single_input_hash_varlen(TestKeyDistribution distribution) {
  SingleInputTest impl(distribution, kVarlenMinHash, kVarlenMaxHash, kVarlenMaxLogHash);
  impl.prepare_inputs(hash_varlen_populate);
  impl.merge_inputs(storage::kHashStorage, hash_varlen_verify);
}

void test_multi_inputs_hash_varlen(TestKeyDistribution distribution) {
  MultiInputsTest impl(
    "test_multi_inputs_hash_varlen_",
    distribution,
    kVarlenMinHash,
    kVarlenMaxHash,
    kVarlenMaxLogHash);
  impl.prepare_inputs(hash_varlen_populate);
  impl.merge_inputs(storage::kHashStorage, hash_varlen_verify);
}

///////////////////////////////////////////////////
/// Masstree testcases
///////////////////////////////////////////////////
void masstree_normalized_populate(log::RecordLogType* log, uint64_t key, const char* payload) {
  char key_be[sizeof(storage::masstree::KeySlice)];
  assorted::write_bigendian<uint64_t>(key, key_be);
  auto* entry = reinterpret_cast<storage::masstree::MasstreeInsertLogType*>(log);
  entry->populate(kStorageId, key_be, sizeof(storage::masstree::KeySlice), payload, kPayload);
}
void masstree_normalized_verify(const log::RecordLogType* log, uint64_t key, const char* payload) {
  const auto* entry = reinterpret_cast<const storage::masstree::MasstreeInsertLogType*>(log);
  char key_be[sizeof(storage::masstree::KeySlice)];
  assorted::write_bigendian<uint64_t>(key, key_be);
  EXPECT_EQ(sizeof(storage::masstree::KeySlice), entry->key_length_);
  EXPECT_EQ(key, entry->get_first_slice());
  EXPECT_EQ(0, std::memcmp(entry->get_key(), key_be, sizeof(storage::masstree::KeySlice)));
  EXPECT_EQ(kPayload, entry->payload_count_);
  EXPECT_EQ(0, std::memcmp(entry->get_payload(), payload, kPayload));
}

void test_single_input_masstree_normalized(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(storage::masstree::KeySlice);
  uint16_t length = storage::masstree::MasstreeInsertLogType::calculate_log_length(kLen, kPayload);
  SingleInputTest impl(distribution, kLen, kLen, length);
  impl.prepare_inputs(masstree_normalized_populate);
  impl.merge_inputs(storage::kMasstreeStorage, masstree_normalized_verify);
}

void test_multi_inputs_masstree_normalized(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(storage::masstree::KeySlice);
  uint16_t length = storage::masstree::MasstreeInsertLogType::calculate_log_length(kLen, kPayload);
  MultiInputsTest impl("test_multi_inputs_masstree_normalized_", distribution, kLen, kLen, length);
  impl.prepare_inputs(masstree_normalized_populate);
  impl.merge_inputs(storage::kMasstreeStorage, masstree_normalized_verify);
}

// varlen key is a bit tricky. test requirements:
//  1) involve keys whose length are less than or larger than 8 bytes
//  2) involve keys whose first 8 bytes are not enough to determine the comparison result
//  3) yet, the key and its ordinal after sorting is easy to understand for humans (for debugging)
// let's do the following:
// - "key / 100" is the "0"-padded 4 digits at beginning. We assume "key" fits in 6 digits.
// - the last digit of key/100 also is the number of spacing (#) after that
// - then key % 100 is appended as "0"-padded 2 digits
// For example, key=12345 becomes "0123###45".
// longest key is 4 + 9 + 2 = 15 bytes. shortest key is 4 + 2 = 6 bytes.
// some key is not differentiated in the first 8 bytes, eg ("0004####01" vs "0004####02")
uint16_t construct_varlen_masstree(uint64_t int_key, char* key) {
  static_assert(kLogsPerInput * 2U < 1000000U, "doesn't fit in 6 digits!");
  uint32_t head = int_key / 100;
  key[0] = '0' + ((head / 1000) % 10);
  key[1] = '0' + ((head / 100) % 10);
  key[2] = '0' + ((head / 10) % 10);
  key[3] = '0' + (head % 10);
  uint16_t len = 4;
  for (uint16_t i = 0; i < (head % 10U); ++i) {
    key[len] = '#';
    ++len;
  }
  uint32_t tail = int_key % 100;
  key[len] = '0' + ((tail / 10) % 10);
  ++len;
  key[len] = '0' + (tail % 10);
  ++len;
  return len;
}
const uint16_t kVarlenMinMasstree = 4 + 0 + 2;
const uint16_t kVarlenMaxMasstree = 4 + 9 + 2;
const uint16_t kVarlenMaxLogMasstree
    = storage::masstree::MasstreeInsertLogType::calculate_log_length(kVarlenMaxMasstree, kPayload);

void masstree_varlen_populate(log::RecordLogType* log, uint64_t int_key, const char* payload) {
  char key[16];
  uint16_t len = construct_varlen_masstree(int_key, key);
  auto* entry = reinterpret_cast<storage::masstree::MasstreeInsertLogType*>(log);
  entry->populate(kStorageId, key, len, payload, kPayload);
}
void masstree_varlen_verify(const log::RecordLogType* log, uint64_t int_key, const char* payload) {
  char key[16];
  uint16_t len = construct_varlen_masstree(int_key, key);
  const auto* entry = reinterpret_cast<const storage::masstree::MasstreeInsertLogType*>(log);
  EXPECT_EQ(len, entry->key_length_);
  EXPECT_EQ(0, std::memcmp(entry->get_key(), key, len));
  EXPECT_EQ(kPayload, entry->payload_count_);
  EXPECT_EQ(0, std::memcmp(entry->get_payload(), payload, kPayload));
}

void test_single_input_masstree_varlen(TestKeyDistribution distribution) {
  SingleInputTest impl(distribution, kVarlenMinMasstree, kVarlenMaxMasstree, kVarlenMaxLogMasstree);
  impl.prepare_inputs(masstree_varlen_populate);
  impl.merge_inputs(storage::kMasstreeStorage, masstree_varlen_verify);
}

void test_multi_inputs_masstree_varlen(TestKeyDistribution distribution) {
  MultiInputsTest impl(
    "test_multi_inputs_masstree_varlen_",
    distribution,
    kVarlenMinMasstree,
    kVarlenMaxMasstree,
    kVarlenMaxLogMasstree);
  impl.prepare_inputs(masstree_varlen_populate);
  impl.merge_inputs(storage::kMasstreeStorage, masstree_varlen_verify);
}

TEST(MergeSortTestTest, SingleInputDistinctKeyArray) {
  test_single_inputs_array(kDistinctKey);
}
TEST(MergeSortTestTest, SingleInputDistinctEpochArray) {
  test_single_inputs_array(kDistinctEpoch);
}
TEST(MergeSortTestTest, SingleInputDistinctOrdinalArray) {
  test_single_inputs_array(kDistinctOrdinal);
}
TEST(MergeSortTestTest, SingleInputDuplicatesArray) {
  test_single_inputs_array(kDuplicates);
}

TEST(MergeSortTestTest, MultiInputsDistinctKeyArray) {
  test_multi_inputs_array(kDistinctKey);
}
TEST(MergeSortTestTest, MultiInputsDistinctEpochArray) {
  test_multi_inputs_array(kDistinctEpoch);
}
TEST(MergeSortTestTest, MultiInputsDistinctOrdinalArray) {
  test_multi_inputs_array(kDistinctOrdinal);
}
TEST(MergeSortTestTest, MultiInputsDuplicatesArray) {
  test_multi_inputs_array(kDuplicates);
}

TEST(MergeSortTestTest, SingleInputDistinctKeyHashFixlen) {
  test_single_input_hash_fixlen(kDistinctKey);
}
TEST(MergeSortTestTest, SingleInputDistinctEpochHashFixlen) {
  test_single_input_hash_fixlen(kDistinctEpoch);
}
TEST(MergeSortTestTest, SingleInputDistinctOrdinalHashFixlen) {
  test_single_input_hash_fixlen(kDistinctOrdinal);
}
TEST(MergeSortTestTest, SingleInputDuplicatesHashFixlen) {
  test_single_input_hash_fixlen(kDuplicates);
}

TEST(MergeSortTestTest, MultiInputsDistinctKeyHashFixlen) {
  test_multi_inputs_hash_fixlen(kDistinctKey);
}
TEST(MergeSortTestTest, MultiInputsDistinctEpochHashFixlen) {
  test_multi_inputs_hash_fixlen(kDistinctEpoch);
}
TEST(MergeSortTestTest, MultiInputsDistinctOrdinalHashFixlen) {
  test_multi_inputs_hash_fixlen(kDistinctOrdinal);
}
TEST(MergeSortTestTest, MultiInputsDuplicatesHashFixlen) {
  test_multi_inputs_hash_fixlen(kDuplicates);
}

TEST(MergeSortTestTest, SingleInputDistinctKeyHashVarlen) {
  test_single_input_hash_varlen(kDistinctKey);
}
TEST(MergeSortTestTest, SingleInputDistinctEpochHashVarlen) {
  test_single_input_hash_varlen(kDistinctEpoch);
}
TEST(MergeSortTestTest, SingleInputDistinctOrdinalHashVarlen) {
  test_single_input_hash_varlen(kDistinctOrdinal);
}
TEST(MergeSortTestTest, SingleInputDuplicatesHashVarlen) {
  test_single_input_hash_varlen(kDuplicates);
}

TEST(MergeSortTestTest, MultiInputsDistinctKeyHashVarlen) {
  test_multi_inputs_hash_varlen(kDistinctKey);
}
TEST(MergeSortTestTest, MultiInputsDistinctEpochHashVarlen) {
  test_multi_inputs_hash_varlen(kDistinctEpoch);
}
TEST(MergeSortTestTest, MultiInputsDistinctOrdinalHashVarlen) {
  test_multi_inputs_hash_varlen(kDistinctOrdinal);
}
TEST(MergeSortTestTest, MultiInputsDuplicatesHashVarlen) {
  test_multi_inputs_hash_varlen(kDuplicates);
}

TEST(MergeSortTestTest, SingleInputDistinctKeyMasstreeNormalized) {
  test_single_input_masstree_normalized(kDistinctKey);
}
TEST(MergeSortTestTest, SingleInputDistinctEpochMasstreeNormalized) {
  test_single_input_masstree_normalized(kDistinctEpoch);
}
TEST(MergeSortTestTest, SingleInputDistinctOrdinalMasstreeNormalized) {
  test_single_input_masstree_normalized(kDistinctOrdinal);
}
TEST(MergeSortTestTest, SingleInputDuplicatesMasstreeNormalized) {
  test_single_input_masstree_normalized(kDuplicates);
}

TEST(MergeSortTestTest, MultiInputsDistinctKeyMasstreeNormalized) {
  test_multi_inputs_masstree_normalized(kDistinctKey);
}
TEST(MergeSortTestTest, MultiInputsDistinctEpochMasstreeNormalized) {
  test_multi_inputs_masstree_normalized(kDistinctEpoch);
}
TEST(MergeSortTestTest, MultiInputsDistinctOrdinalMasstreeNormalized) {
  test_multi_inputs_masstree_normalized(kDistinctOrdinal);
}
TEST(MergeSortTestTest, MultiInputsDuplicatesMasstreeNormalized) {
  test_multi_inputs_masstree_normalized(kDuplicates);
}

TEST(MergeSortTestTest, SingleInputDistinctKeyMasstreeVarlen) {
  test_single_input_masstree_varlen(kDistinctKey);
}
TEST(MergeSortTestTest, SingleInputDistinctEpochMasstreeVarlen) {
  test_single_input_masstree_varlen(kDistinctEpoch);
}
TEST(MergeSortTestTest, SingleInputDistinctOrdinalMasstreeVarlen) {
  test_single_input_masstree_varlen(kDistinctOrdinal);
}
TEST(MergeSortTestTest, SingleInputDuplicatesMasstreeVarlen) {
  test_single_input_masstree_varlen(kDuplicates);
}

TEST(MergeSortTestTest, MultiInputsDistinctKeyMasstreeVarlen) {
  test_multi_inputs_masstree_varlen(kDistinctKey);
}
TEST(MergeSortTestTest, MultiInputsDistinctEpochMasstreeVarlen) {
  test_multi_inputs_masstree_varlen(kDistinctEpoch);
}
TEST(MergeSortTestTest, MultiInputsDistinctOrdinalMasstreeVarlen) {
  test_multi_inputs_masstree_varlen(kDistinctOrdinal);
}
TEST(MergeSortTestTest, MultiInputsDuplicatesMasstreeVarlen) {
  test_multi_inputs_masstree_varlen(kDuplicates);
}

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MergeSortTestTest, foedus.snapshot);
