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
    // the install() of hashtable in snapshot cache is opportunitstic.
    // in order to make it wait-free, we tolerate a chance of other thread overwriting what is
    // written. thus, we confirm whether it's installed there, and install it again if not.
    assorted::memory_fence_acq_rel();
    content_id = hashtable->find(pointer);
    if (content_id == 0) {
      std::cout << "Interesting, concurrent threads have just overwritten what I wrote."
        << "id=" << id << ", i=" << i << std::endl;
      // we assume the same unlucky wouldn't happen again. it is possible, but negligible in the
      // test case. even if it happens, it's not a bug, btw.
      EXPECT_EQ(kErrorCodeOk, hashtable->install(pointer, i * 3 + 42));
    } else {
      EXPECT_EQ(i * 3U + 42U, content_id);
    }

    // we once had a testcase failure caused by the above (enable the 1000-time repeat below,
    // you will see the message above once in hundred times).
  }
}

TEST(HashTableTest, RandomMultiThread) {
  // there is a hard-to-reproduce bug. repeat many times just for repro. do not push it to repo!
  // for (int aaa = 0; aaa < 1000; ++aaa) {
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
  // }
}


void test_evict(const uint32_t kHashTableSize, const uint32_t kCounts) {
  CacheHashtable hashtable(kHashTableSize, 0);
  bool collides[kCounts];
  bool exists[kCounts];
  std::memset(collides, 0, sizeof(bool) * kCounts);
  std::memset(exists, 0, sizeof(bool) * kCounts);
  uint32_t collisions = 0;
  for (uint32_t i = 0; i < kCounts; ++i) {
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, i % 3, i / 3);
    ContentId content_id = hashtable.find(pointer);
    if (content_id > 0) {
      std::cout << "hash collision. rare, but possible. "
        << i << " collided with " << (content_id - 42U) << std::endl;
      ++collisions;
      collides[i] = true;
      continue;
    }
    ErrorCode install_ret = hashtable.install(pointer, i + 42);
    if (install_ret != kErrorCodeOk) {
      EXPECT_EQ(kErrorCodeCacheTooManyOverflow, install_ret);
      std::cout << "oh yeah, too many overflows. this is intented: " << i << std::endl;
      // give up inserting them, too.
      ++collisions;
      collides[i] = true;
      continue;
    }
    exists[i] = true;
    // access it for a slightly random times to add up the refcount
    for (uint32_t rep = 0; rep < 1U + (i % 7U); ++rep) {
      ContentId found = hashtable.find(pointer);
      EXPECT_EQ(i + 42U, found) << i;
    }
  }
  COERCE_ERROR(hashtable.verify_single_thread());
  CacheHashtable::Stat stat = hashtable.get_stat_single_thread();
  std::cout << "before: normal=" << stat.normal_entries_
    << ", overflow=" << stat.overflow_entries_ << std::endl;
  EXPECT_EQ(kCounts, stat.normal_entries_ + stat.overflow_entries_ + collisions);

  // evict some of them over a few times.
  uint32_t evicted_total = 0;
  for (uint32_t rep = 0; rep < 3U; ++rep) {
    uint32_t evicted[kCounts];
    std::memset(evicted, 0, sizeof(evicted));
    CacheHashtable::EvictArgs args = { kCounts / 10, 0, evicted };
    hashtable.evict(&args);
    EXPECT_GE(args.evicted_count_, args.target_count_ * 8U / 10U);  // might be a bit smaller
    EXPECT_LE(args.evicted_count_, args.target_count_ * 12U / 10U);  // might be a bit larger
    for (uint32_t i = 0; i < args.evicted_count_; ++i) {
      EXPECT_NE(0, evicted[i]) << i;
      EXPECT_LE(42U, evicted[i]) << i;
      EXPECT_GT(42U + kCounts, evicted[i]) << i;
      EXPECT_TRUE(exists[evicted[i] - 42U]) << i;
      exists[evicted[i] - 42U] = false;
    }
    evicted_total += args.evicted_count_;
  }

  COERCE_ERROR(hashtable.verify_single_thread());
  stat = hashtable.get_stat_single_thread();
  std::cout << "after: normal=" << stat.normal_entries_
    << ", overflow=" << stat.overflow_entries_ << std::endl;
  EXPECT_EQ(kCounts, stat.normal_entries_ + stat.overflow_entries_ + collisions + evicted_total);

  for (uint32_t i = 0; i < kCounts; ++i) {
    if (collides[i]) {
      // in this case, we are not sure the entry it collided is still there or not. can't test.
      continue;
    }
    storage::SnapshotPagePointer pointer;
    pointer = storage::to_snapshot_page_pointer(1, i % 3, i / 3);
    ContentId content_id = hashtable.find(pointer);
    if (exists[i]) {
      EXPECT_EQ(i + 42U, content_id) << i;
    } else {
      // usually content_id should be 0 here. However,
      // the returned content_id might be someone else's if our "tag" formula causes a collision
      if (content_id > 0) {
        uint32_t other = content_id - 42U;
        storage::SnapshotPagePointer other_pointer
          = storage::to_snapshot_page_pointer(1, other % 3, other / 3);
        PageIdTag other_tag = HashFunc::get_tag(other_pointer);
        PageIdTag my_tag = HashFunc::get_tag(pointer);
        EXPECT_EQ(my_tag, other_tag) << i;
        std::cout << "tag collision? me=" << i << ":" << my_tag
          << ", other=" << other << ":" << other_tag << std::endl;
      }
    }
  }
}

TEST(HashTableTest, EvictLittleEntries) { test_evict(12345, 10); }
TEST(HashTableTest, EvictNoOverflow) { test_evict(12345, 1000); }
TEST(HashTableTest, EvictLittleOverflow) { test_evict(12345, 2000); }

// these take long time if run with the same scale. so, one tenth.
TEST(HashTableTest, EvictManyOverflow) { test_evict(1234, 500); }
TEST(HashTableTest, EvictMostlyOverflow) { test_evict(1234, 1000); }
}  // namespace cache
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashTableTest, foedus.cache);
