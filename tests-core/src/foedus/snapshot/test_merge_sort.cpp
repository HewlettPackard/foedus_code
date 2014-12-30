/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
// #include <gperftools/profiler.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "foedus/test_common.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/log_buffer.hpp"
#include "foedus/snapshot/merge_sort.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/array/array_log_types.hpp"
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
    uint64_t max_log_length)
    : distribution_(distribution),
      max_log_length_(max_log_length),
      inmemory_buffer_(nullptr) {
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
    uint16_t shortest_key_length,
    uint16_t longest_key_length,
    VERIFY verify) {
    memory::AlignedMemory work_memory;
    work_memory.alloc(1U << 21, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);

    MergeSort merge_sort(
      kStorageId,
      storage_type,
      Epoch(1),
      shortest_key_length,
      longest_key_length,
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

        EXPECT_GE(pos.key_length_, shortest_key_length);
        EXPECT_LE(pos.key_length_, longest_key_length);

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
      total_processed += merge_sort.get_current_count();
    }
    COERCE_ERROR(merge_sort.uninitialize());
    EXPECT_EQ(kLogsPerInput * get_input_count(), total_processed);
  }

  TestKeyDistribution distribution_;
  uint64_t capacity_;
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
    uint64_t max_log_length)
    : TestBase(distribution, max_log_length) {
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
    inmemory_buffer_->set_current_block(kStorageId, kLogsPerInput, 0, cur);
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
    uint64_t max_log_length)
    : TestBase(distribution, max_log_length),
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
    inmemory_buffer_->set_current_block(kStorageId, kLogsPerInput, 0, cur);

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
    io_buffer_->set_current_block(kStorageId, kLogsPerInput, 0, cur);
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
  SingleInputTest impl(distribution, length);
  impl.prepare_inputs(array_populate);
  impl.merge_inputs(storage::kArrayStorage, kLen, kLen, array_verify);
}

void test_multi_inputs_array(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(storage::array::ArrayOffset);
  uint16_t length = storage::array::ArrayOverwriteLogType::calculate_log_length(kPayload);
  MultiInputsTest impl("test_multi_inputs_array_", distribution, length);
  impl.prepare_inputs(array_populate);
  impl.merge_inputs(storage::kArrayStorage, kLen, kLen, array_verify);
}


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
  SingleInputTest impl(distribution, length);
  impl.prepare_inputs(masstree_normalized_populate);
  impl.merge_inputs(storage::kMasstreeStorage, kLen, kLen, masstree_normalized_verify);
}

void test_multi_inputs_masstree_normalized(TestKeyDistribution distribution) {
  const uint16_t kLen = sizeof(storage::masstree::KeySlice);
  uint16_t length = storage::masstree::MasstreeInsertLogType::calculate_log_length(kLen, kPayload);
  MultiInputsTest impl("test_multi_inputs_masstree_normalized_", distribution, length);
  impl.prepare_inputs(masstree_normalized_populate);
  impl.merge_inputs(storage::kMasstreeStorage, kLen, kLen, masstree_normalized_verify);
}

// varlen key is a bit tricky. test requirements:
//  1) involve keys whose length are less than or larger than 8 bytes
//  2) involve keys whose first 8 bytes are not enough to determine the comparison result
//  3) yet, the key and its ordinal after sorting is easy to understand for humans (for debugging)
// let's do the following:
// - "key / 100" is the "0"-padded 4 digits at beginning. We assume "key" fits in 6 digits.
// - the last digit of key/100 also os the number of spacing (#) after that
// - then key % 100 is appended as "0"-padded 2 digits
// For example, key=12345 becomes "0123###45".
// largest key is 4 + 9 + 2 = 15 bytes. shortest key is 4 + 2 = 6 bytes.
// some key is not differentiated in the first 8 bytes, eg ("0004####01" vs "0004####02")
uint16_t construct_varlen(uint64_t int_key, char* key) {
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
const uint16_t kVarlenMin = 4 + 0 + 2;
const uint16_t kVarlenMax = 4 + 9 + 2;
const uint16_t kVarlenMaxLog
    = storage::masstree::MasstreeInsertLogType::calculate_log_length(kVarlenMax, kPayload);

void masstree_varlen_populate(log::RecordLogType* log, uint64_t int_key, const char* payload) {
  char key[16];
  uint16_t len = construct_varlen(int_key, key);
  auto* entry = reinterpret_cast<storage::masstree::MasstreeInsertLogType*>(log);
  entry->populate(kStorageId, key, len, payload, kPayload);
}
void masstree_varlen_verify(const log::RecordLogType* log, uint64_t int_key, const char* payload) {
  char key[16];
  uint16_t len = construct_varlen(int_key, key);
  const auto* entry = reinterpret_cast<const storage::masstree::MasstreeInsertLogType*>(log);
  EXPECT_EQ(len, entry->key_length_);
  EXPECT_EQ(0, std::memcmp(entry->get_key(), key, len));
  EXPECT_EQ(kPayload, entry->payload_count_);
  EXPECT_EQ(0, std::memcmp(entry->get_payload(), payload, kPayload));
}

void test_single_input_masstree_varlen(TestKeyDistribution distribution) {
  SingleInputTest impl(distribution, kVarlenMaxLog);
  impl.prepare_inputs(masstree_varlen_populate);
  impl.merge_inputs(storage::kMasstreeStorage, kVarlenMin, kVarlenMax, masstree_varlen_verify);
}

void test_multi_inputs_masstree_varlen(TestKeyDistribution distribution) {
  MultiInputsTest impl("test_multi_inputs_masstree_varlen_", distribution, kVarlenMaxLog);
  impl.prepare_inputs(masstree_varlen_populate);
  impl.merge_inputs(storage::kMasstreeStorage, kVarlenMin, kVarlenMax, masstree_varlen_verify);
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
