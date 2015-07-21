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

#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <utility>

#include "foedus/test_common.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/cache_hashtable.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace cache {
DEFINE_TEST_CASE_PACKAGE(HashFuncTest, foedus.cache);

void test_instantiate(uint32_t physical_buckets) {
  cache::HashFunc func(physical_buckets);
  EXPECT_EQ(physical_buckets, func.physical_buckets_);
  EXPECT_LE(func.logical_buckets_, physical_buckets) << func;
}

TEST(HashFuncTest, Instantiate) {
  test_instantiate(1234);
  test_instantiate(12345);
  test_instantiate(123456);
  test_instantiate(1234567);
  test_instantiate(1 << 13);
  test_instantiate((1 << 13) - 1);
  test_instantiate((1 << 13) + 1);
}

void test_not_dup(const cache::HashFunc& func, uint64_t key,  std::set<uint32_t>* existing) {
  uint32_t hash = func.get_bucket_number(key);
  EXPECT_EQ(existing->end(), existing->find(hash))
    << "key=" << assorted::Hex(key, 16) << ":hash=" << assorted::Hex(hash, 8) << ":func=" << func;
  existing->insert(hash);
}

void test_fixed(uint32_t table_size) {
  cache::HashFunc func(table_size);
  std::set<uint32_t> existing;
  test_not_dup(func, 0, &existing);
  test_not_dup(func, 1, &existing);
  test_not_dup(func, 2, &existing);
  test_not_dup(func, 3, &existing);
  test_not_dup(func, 8, &existing);
  test_not_dup(func, 9, &existing);
  test_not_dup(func, 10, &existing);
  test_not_dup(func, 255, &existing);
  test_not_dup(func, 257, &existing);
  test_not_dup(func, 12345, &existing);
  test_not_dup(func, 23456, &existing);
  test_not_dup(func, (1ULL << 63) + 1, &existing);
  test_not_dup(func, -1ULL, &existing);
}

TEST(HashFuncTest, Fixed) {
  test_fixed(12345);
  test_fixed(1024);
  test_fixed(55123);
  test_fixed(1U << 23);
}

void test_random(uint32_t table_size) {
  cache::HashFunc func(table_size);
  std::set<uint32_t> existing;
  // random, but deterministic for reproducibility.
  assorted::UniformRandom rnd(table_size);
  const uint32_t kReps = std::min<uint32_t>(func.logical_buckets_ / 2U, 1U << 15);
  uint32_t dups = 0;
  for (uint32_t rep = 0; rep < kReps; ++rep) {
    uint64_t key = rnd.next_uint64();
    uint32_t hash = func.get_bucket_number(key);
    if (existing.find(hash) != existing.end()) {
      ++dups;
    } else {
      existing.insert(hash);
    }
  }

  // f(x) = x / table_size: probability to collide when there are already x entries.
  // g(x): total number of collisions for x reps.
  // with an approximation f(x) << 1, g(x) ~= sum_0^x{f(0)} ~= x^2/ 2*table_size
  // note that the approximation doesn't hold when kReps approaches table_size.
  double expected_collisions = static_cast<double>(kReps) * kReps / (2 * func.logical_buckets_);
  EXPECT_LT(dups, expected_collisions * 1.2 + 5) << "func=" << func;
  EXPECT_GT(dups, expected_collisions * 0.8 - 5) << "func=" << func;
}


TEST(HashFuncTest, Random) {
  test_random(44321);
  test_random(1024);
  test_random(55123);
  test_random(5634452);
  test_random(1U << 20);
  test_random(1U << 23);
}

void test_skewed_page_ids(uint32_t table_size) {
  cache::HashFunc func(table_size);
  std::map<uint32_t, uint64_t> existing;
  // we create a skew in byte representation.
  const uint32_t kReps = std::min<uint32_t>(func.logical_buckets_ / 2U, 1U << 15);
  uint32_t dups = 0;
  const uint16_t snapshot_id = 1;
  const uint16_t nodes = 4;
  for (uint32_t rep = 0; rep < kReps; ++rep) {
    // the "offset" part is sequential on purpose. this actually happens often.
    uint8_t node = rep % nodes;
    uint64_t offset = rep / nodes;
    storage::SnapshotPagePointer key = storage::to_snapshot_page_pointer(snapshot_id, node, offset);
    uint32_t hash = func.get_bucket_number(key);
    if (existing.find(hash) != existing.end()) {
      /*
      std::cout << "Dup(rep=" << rep << ",hash=" << assorted::Hex(hash, 8)
        << "), key=" << assorted::Hex(key, 16) << " other="
        << assorted::Hex(existing.find(hash)->second, 16) << std::endl;
      */
      ++dups;
    } else {
      existing.insert(std::pair<uint32_t, uint64_t>(hash, key));
    }
  }

  double expected_collisions = static_cast<double>(kReps) * kReps / (2 * func.logical_buckets_);
  // same, but more tolerant. this is a harder case.
  EXPECT_LT(dups, expected_collisions * 1.4 + 5) << "kReps=" << kReps << ":func=" << func;
  // As this is not uniform random, "GT" testcase doesn't make sense. it often happens that
  // a good hash function has no collisions for such inputs.

  // On the other hand, we are guaranteed to be screwed if there is an opposite skew:
  // significant bits of SnapshotPagePointer (snapshot ID/node) are highly sequential.
  // it's fine. it will never happen. nothing is perfect...
}

TEST(HashFuncTest, SkewedPageIds) {
  test_skewed_page_ids(44321);
  test_skewed_page_ids(1024);
  test_skewed_page_ids(55123);
  test_skewed_page_ids(5634452);
  test_skewed_page_ids(1U << 20);
  test_skewed_page_ids(1U << 23);
}

}  // namespace cache
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashFuncTest, foedus.cache);
