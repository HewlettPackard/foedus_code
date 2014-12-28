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

void test_single_input_array(TestKeyDistribution distribution) {
  uint16_t length = storage::array::ArrayOverwriteLogType::calculate_log_length(kPayload);
  const uint64_t capacity = length * kLogsPerInput;
  memory::AlignedMemory aligned_memory;
  aligned_memory.alloc(capacity, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
  uint64_t cur = 0;
  char* buf = reinterpret_cast<char*>(aligned_memory.get_block());
  char payload[kPayload];
  std::memset(payload, 0, kPayload);
  for (uint64_t i = 0; i < kLogsPerInput; ++i) {
    auto* entry = reinterpret_cast<storage::array::ArrayOverwriteLogType*>(buf + cur);
    std::memcpy(payload, &i, sizeof(uint64_t));
    entry->populate(kStorageId, to_key(i, distribution), payload, 0, kPayload);
    entry->header_.xct_id_.set(to_epoch(i, distribution), to_in_epoch_ordinal(i, distribution));
    ASSERT_ND(entry->header_.log_length_ == length);
    cur += entry->header_.log_length_;
  }
  ASSERT_ND(cur == capacity);

  // ::ProfilerStart(to_string(distribution).c_str());

  InMemorySortedBuffer sorted_buffer(buf, capacity);
  sorted_buffer.set_current_block(kStorageId, kLogsPerInput, 0, capacity);
  memory::AlignedMemory work_memory;
  work_memory.alloc(1U << 21, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
  SortedBuffer* input_array[1];
  input_array[0] = &sorted_buffer;
  MergeSort merge_sort(
    kStorageId,
    storage::kArrayStorage,
    Epoch(1),
    sizeof(storage::array::ArrayOffset),
    sizeof(storage::array::ArrayOffset),
    input_array,
    1,
    8,
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
      uint64_t key = to_key(accumulative_i, distribution);
      uint16_t epoch = to_epoch(accumulative_i, distribution);
      uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution);
      SCOPED_TRACE(testing::Message() << "i=" << accumulative_i << ", key=" << key
          << ", epoch=" << epoch << ", ordinal=" << ordinal);

      EXPECT_EQ(key, merge_sort.get_sort_entries()[i].get_key());
      EXPECT_EQ(i, merge_sort.get_sort_entries()[i].get_position());
      const MergeSort::PositionEntry& pos = merge_sort.get_position_entries()[i];
      EXPECT_EQ(0, pos.input_index_);
      EXPECT_EQ(sizeof(storage::array::ArrayOffset), pos.key_length_);
      // this test is valid because this is a single-input testcase without window-shift.
      EXPECT_EQ(to_buffer_position(length * accumulative_i), pos.input_position_);
      EXPECT_EQ(merge_sort.resolve_sort_position(i), merge_sort.resolve_merged_position(i));

      const auto* log = reinterpret_cast<const storage::array::ArrayOverwriteLogType*>(
        merge_sort.resolve_merged_position(i));
      EXPECT_EQ(key, log->offset_);
      EXPECT_EQ(epoch, log->header_.xct_id_.get_epoch_int());
      EXPECT_EQ(ordinal, log->header_.xct_id_.get_ordinal());
      EXPECT_EQ(0, log->payload_offset_);
      EXPECT_EQ(kPayload, log->payload_count_);
      EXPECT_EQ(0, std::memcmp(log->payload_, &accumulative_i, sizeof(uint64_t)));
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
        uint64_t key = to_key(accumulative_i, distribution);
        uint16_t epoch = to_epoch(accumulative_i, distribution);
        uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution);
        SCOPED_TRACE(testing::Message() << "i=" << accumulative_i << ", key=" << key
            << ", epoch=" << epoch << ", ordinal=" << ordinal);
        EXPECT_EQ(key, logs[j]->offset_);
        EXPECT_EQ(epoch, logs[j]->header_.xct_id_.get_epoch_int());
        EXPECT_EQ(ordinal, logs[j]->header_.xct_id_.get_ordinal());
        EXPECT_EQ(0, logs[j]->payload_offset_);
        EXPECT_EQ(kPayload, logs[j]->payload_count_);
        EXPECT_EQ(0, std::memcmp(logs[j]->payload_, &accumulative_i, sizeof(uint64_t)));
      }
      i+= fetched;
      ASSERT_ND(i <= merge_sort.get_current_count());
    }
    total_processed += merge_sort.get_current_count();
  }
  // ::ProfilerStop();
  COERCE_ERROR(merge_sort.uninitialize());
  EXPECT_EQ(kLogsPerInput, total_processed);
}


void test_multi_inputs_array(TestKeyDistribution distribution) {
  uint16_t length = storage::array::ArrayOverwriteLogType::calculate_log_length(kPayload);
  const uint64_t capacity = length * kLogsPerInput;

  // in-memory buffer (1st buffer)
  memory::AlignedMemory aligned_memory;
  aligned_memory.alloc(capacity, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
  uint64_t cur = 0;
  char* buf = reinterpret_cast<char*>(aligned_memory.get_block());
  char payload[kPayload];
  std::memset(payload, 0, kPayload);
  for (uint64_t i = 0; i < kLogsPerInput * 2U; i += 2) {  // even numbers
    auto* entry = reinterpret_cast<storage::array::ArrayOverwriteLogType*>(buf + cur);
    std::memcpy(payload, &i, sizeof(uint64_t));
    entry->populate(kStorageId, to_key(i, distribution), payload, 0, kPayload);
    entry->header_.xct_id_.set(to_epoch(i, distribution), to_in_epoch_ordinal(i, distribution));
    ASSERT_ND(entry->header_.log_length_ == length);
    cur += entry->header_.log_length_;
  }
  ASSERT_ND(cur == capacity);

  InMemorySortedBuffer sorted_buffer(buf, capacity);
  sorted_buffer.set_current_block(kStorageId, kLogsPerInput, 0, capacity);

  // on-disk buffer (2nd buffer)
  memory::AlignedMemory io_memory;
  io_memory.alloc(capacity, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
  cur = 0;
  char* buf2 = reinterpret_cast<char*>(io_memory.get_block());
  for (uint64_t i = 1; i < kLogsPerInput * 2U; i += 2) {  // odd numbers
    auto* entry = reinterpret_cast<storage::array::ArrayOverwriteLogType*>(buf2 + cur);
    std::memcpy(payload, &i, sizeof(uint64_t));
    entry->populate(kStorageId, to_key(i, distribution), payload, 0, kPayload);
    entry->header_.xct_id_.set(to_epoch(i, distribution), to_in_epoch_ordinal(i, distribution));
    ASSERT_ND(entry->header_.log_length_ == length);
    cur += entry->header_.log_length_;
  }
  ASSERT_ND(cur == capacity);

  std::string file_name;
  file_name = "test_multi_inputs_array_";
  file_name += to_string(distribution);
  fs::Path path(get_random_tmp_file_path(file_name));
  EXPECT_TRUE(fs::create_directories(path.parent_path()));

  fs::DirectIoFile file(path);
  COERCE_ERROR_CODE(file.open(true, true, true, true));
  COERCE_ERROR_CODE(file.write_raw(capacity, buf2));
  COERCE_ERROR_CODE(file.sync());

  // open it again to read from the beginning
  EXPECT_TRUE(file.close());
  COERCE_ERROR_CODE(file.open(true, false, false, false));

  // smaller buffer size on purpose. causes window move
  memory::AlignedMemorySlice slice(&io_memory, 0, capacity / 4U);
  COERCE_ERROR_CODE(file.read(capacity / 4U, slice));
  DumpFileSortedBuffer disk_buffer(&file, slice);
  disk_buffer.set_current_block(kStorageId, kLogsPerInput, 0, capacity);

  memory::AlignedMemory work_memory;
  work_memory.alloc(1U << 21, 1U << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
  SortedBuffer* input_array[2];
  input_array[0] = &sorted_buffer;
  input_array[1] = &disk_buffer;
  MergeSort merge_sort(
    kStorageId,
    storage::kArrayStorage,
    Epoch(1),
    sizeof(storage::array::ArrayOffset),
    sizeof(storage::array::ArrayOffset),
    input_array,
    2,
    8,
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
      uint64_t key = to_key(accumulative_i, distribution);
      uint16_t epoch = to_epoch(accumulative_i, distribution);
      uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution);
      SCOPED_TRACE(testing::Message() << "i=" << accumulative_i << ", key=" << key
          << ", epoch=" << epoch << ", ordinal=" << ordinal);

      uint32_t position = merge_sort.get_sort_entries()[i].get_position();
      ASSERT_ND(position < merge_sort.get_current_count());
      EXPECT_EQ(key, merge_sort.get_sort_entries()[i].get_key());
      const MergeSort::PositionEntry& pos = merge_sort.get_position_entries()[position];
      EXPECT_EQ(accumulative_i % 2U, pos.input_index_);
      EXPECT_EQ(sizeof(storage::array::ArrayOffset), pos.key_length_);

      const auto* log = reinterpret_cast<const storage::array::ArrayOverwriteLogType*>(
        merge_sort.resolve_sort_position(i));
      EXPECT_EQ(key, log->offset_);
      EXPECT_EQ(epoch, log->header_.xct_id_.get_epoch_int());
      EXPECT_EQ(ordinal, log->header_.xct_id_.get_ordinal());
      EXPECT_EQ(0, log->payload_offset_);
      EXPECT_EQ(kPayload, log->payload_count_);
      EXPECT_EQ(0, std::memcmp(log->payload_, &accumulative_i, sizeof(uint64_t)));
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
        uint64_t key = to_key(accumulative_i, distribution);
        uint16_t epoch = to_epoch(accumulative_i, distribution);
        uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution);
        SCOPED_TRACE(testing::Message() << "i=" << accumulative_i << ", key=" << key
            << ", epoch=" << epoch << ", ordinal=" << ordinal);
        EXPECT_EQ(key, logs[j]->offset_);
        EXPECT_EQ(epoch, logs[j]->header_.xct_id_.get_epoch_int());
        EXPECT_EQ(ordinal, logs[j]->header_.xct_id_.get_ordinal());
        EXPECT_EQ(0, logs[j]->payload_offset_);
        EXPECT_EQ(kPayload, logs[j]->payload_count_);
        EXPECT_EQ(0, std::memcmp(logs[j]->payload_, &accumulative_i, sizeof(uint64_t)));
      }
      i+= fetched;
      ASSERT_ND(i <= merge_sort.get_current_count());
    }
    total_processed += merge_sort.get_current_count();
  }
  COERCE_ERROR(merge_sort.uninitialize());
  EXPECT_EQ(kLogsPerInput * 2U, total_processed);

  EXPECT_TRUE(file.close());
  fs::remove_all(path.parent_path());
}

TEST(MergeSortTestTest, SingleInputDistinctKeyArray) {
  test_single_input_array(kDistinctKey);
}
TEST(MergeSortTestTest, SingleInputDistinctEpochArray) {
  test_single_input_array(kDistinctEpoch);
}
TEST(MergeSortTestTest, SingleInputDistinctOrdinalArray) {
  test_single_input_array(kDistinctOrdinal);
}
TEST(MergeSortTestTest, SingleInputDuplicatesArray) {
  test_single_input_array(kDuplicates);
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

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MergeSortTestTest, foedus.snapshot);
