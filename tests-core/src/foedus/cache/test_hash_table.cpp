/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/cache_hashtable.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace cache {
DEFINE_TEST_CASE_PACKAGE(HashTableTest, foedus.cache);

TEST(HashTableTest, Instantiate) {
  CacheHashtable hashtable(1234567, 0);
  EXPECT_EQ(1234567U, hashtable.get_physical_buckets());
  EXPECT_GT(1234567U, hashtable.get_logical_buckets());
}

struct TestContext {
  ErrorCode on_read_callback(
    CacheHashtable* /*table*/,
    storage::SnapshotPagePointer page_id,
    ContentId* content_id) {
    *content_id = storage::extract_local_page_id_from_snapshot_pointer(page_id) + 42;
    return kErrorCodeOk;
  }
};

/** Implementation of PageReadCallback in CacheHashtable */
inline ErrorCode test_read_callback(
  CacheHashtable* table,
  void* context,
  storage::SnapshotPagePointer page_id,
  ContentId* content_id) {
  TestContext* casted = reinterpret_cast<TestContext*>(context);
  return casted->on_read_callback(table, page_id, content_id);
}

inline ErrorCode test_mustnot_happen_callback(
  CacheHashtable* /*table*/,
  void* /*context*/,
  storage::SnapshotPagePointer /*page_id*/,
  ContentId* /*content_id*/) {
  ASSERT_ND(false);
  return kErrorCodeUserDefined;
}

TEST(HashTableTest, Random) {
  TestContext test_context;
  CacheHashtable hashtable(123456, 0);
  for (uint32_t i = 0; i < 10000U; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, 0, i * 3);
    ContentId content_id;
    EXPECT_EQ(
      kErrorCodeOk,
      hashtable.retrieve(pointer, &content_id, test_read_callback, &test_context)) << i;
    EXPECT_EQ(i * 3 + 42, content_id);
  }
  COERCE_ERROR(hashtable.verify_single_thread());

  for (uint32_t i = 0; i < 10000U; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, 0, i * 3);
    ContentId content_id;
    EXPECT_EQ(
      kErrorCodeOk,
      hashtable.retrieve(pointer, &content_id, test_mustnot_happen_callback, nullptr)) << i;
    EXPECT_EQ(i * 3 + 42, content_id);
  }
}

void test_multi_thread(uint32_t id, CacheHashtable* hashtable) {
  TestContext test_context;
  for (uint32_t i = 0 + id * 10000U; i < (id + 1U) * 10000U; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, 0, i * 3);
    ContentId content_id;
    EXPECT_EQ(
      kErrorCodeOk,
      hashtable->retrieve(pointer, &content_id, test_read_callback, &test_context)) << i;
    EXPECT_EQ(i * 3 + 42, content_id);
  }
}

TEST(HashTableTest, RandomMultiThread) {
  const uint32_t kThreads = 4;
  std::vector<std::thread> threads;
  CacheHashtable hashtable(123456, 0);
  for (uint32_t id = 0; id < kThreads; ++id) {
    threads.emplace_back(test_multi_thread, id, &hashtable);
  }

  for (auto& t : threads) {
    t.join();
  }

  COERCE_ERROR(hashtable.verify_single_thread());
  for (uint32_t i = 0; i < 10000U * kThreads; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, 0, i * 3);
    ContentId content_id;
    EXPECT_EQ(
      kErrorCodeOk,
      hashtable.retrieve(pointer, &content_id, test_mustnot_happen_callback, nullptr)) << i;
    EXPECT_EQ(i * 3 + 42, content_id);
  }
}

}  // namespace cache
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashTableTest, foedus.cache);
