/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <cstring>

#include "foedus/test_common.hpp"
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
const uint64_t kLogsPerInput = 1 << 19;
const uint32_t kPayload = 8;
const uint32_t kFetchSize = 4;


enum TestKeyDistribution {
  /** logs have distinct keys */
  kDistinctKey,
  /** logs might have duplicate keys, but can be differentiated with epoch */
  kDistinctEpoch,
  /** logs might have duplicate keys and epoch, but can be differentiated with ordinal */
  kDistinctOrdinal,
  /** logs might have duplicate, but internally differentiated by buffer position. */
  kDuplicates,
};

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
    &work_memory);
  COERCE_ERROR(merge_sort.initialize());
  uint64_t total_processed = 0;
  while (merge_sort.next_window()) {
    for (uint64_t i = 0; i < merge_sort.get_current_count(); ++i) {
      uint64_t accumulative_i = i + total_processed;
      uint64_t offset = to_key(accumulative_i, distribution);
      uint16_t epoch = to_epoch(accumulative_i, distribution);
      uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution);

      EXPECT_EQ(offset, merge_sort.get_sort_entries()[i].get_key());
      EXPECT_EQ(i, merge_sort.get_sort_entries()[i].get_position());
      const MergeSort::PositionEntry& pos = merge_sort.get_position_entries()[i];
      EXPECT_EQ(0, pos.input_index_);
      EXPECT_EQ(sizeof(storage::array::ArrayOffset), pos.key_length_);
      // this test is valid because this is a single-input testcase without window-shift.
      EXPECT_EQ(to_buffer_position(length * accumulative_i), pos.input_position_);
      EXPECT_EQ(merge_sort.resolve_sort_position(i), merge_sort.resolve_merged_position(i));

      const auto* log = reinterpret_cast<const storage::array::ArrayOverwriteLogType*>(
        merge_sort.resolve_merged_position(i));
      EXPECT_EQ(offset, log->offset_);
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
        uint64_t offset = to_key(accumulative_i, distribution);
        uint16_t epoch = to_epoch(accumulative_i, distribution);
        uint32_t ordinal = to_in_epoch_ordinal(accumulative_i, distribution);
        EXPECT_EQ(offset, logs[j]->offset_);
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
  EXPECT_EQ(kLogsPerInput, total_processed);
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
TEST(MergeSortTestTest, SingleInputDuplicates) {
  test_single_input_array(kDuplicates);
}

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MergeSortTestTest, foedus.snapshot);
