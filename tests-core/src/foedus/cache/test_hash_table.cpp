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

TEST(HashTableTest, Random) {
  CacheHashtable hashtable(123456, 0);
  for (uint32_t i = 0; i < 10000U; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, 0, i * 3);
    ContentId content_id = hashtable.find(pointer);
    EXPECT_EQ(0, content_id);
    EXPECT_EQ(kErrorCodeOk, hashtable.install(pointer, i * 3 + 42));
  }
  COERCE_ERROR(hashtable.verify_single_thread());

  for (uint32_t i = 0; i < 10000U; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, 0, i * 3);
    ContentId content_id = hashtable.find(pointer);
    EXPECT_EQ(i * 3U + 42U, content_id);
  }
}

void test_multi_thread(uint32_t id, CacheHashtable* hashtable) {
  for (uint32_t i = 0 + id * 10000U; i < (id + 1U) * 10000U; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, 0, i * 3);

    ContentId content_id = hashtable->find(pointer);
    EXPECT_EQ(0, content_id);
    EXPECT_EQ(kErrorCodeOk, hashtable->install(pointer, i * 3 + 42));
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
    ContentId content_id = hashtable.find(pointer);
    EXPECT_EQ(i * 3U + 42U, content_id);
  }
}

}  // namespace cache
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashTableTest, foedus.cache);
