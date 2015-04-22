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

#include <stdint.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <set>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/storage/hash/hash_combo.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"

namespace foedus {
namespace storage {
namespace hash {
DEFINE_TEST_CASE_PACKAGE(HashHashinateTest, foedus.storage.hash);

TEST(HashHashinateTest, Primitives) {
  uint64_t u64 = 0x1234567890ABCDEFULL;
  EXPECT_EQ(hashinate(u64), hashinate(&u64, sizeof(u64)));
  EXPECT_NE(hashinate(u64), hashinate(&u64, sizeof(uint32_t)));
  EXPECT_NE(hashinate(u64), hashinate<uint64_t>(0ULL));
}

#ifndef NDEBUG
const uint64_t kCollisionReps = 1 << 16;
#else  // NDEBUG
const uint64_t kCollisionReps = 1 << 20;
#endif  // NDEBUG
const uint8_t kTestBinBits = 12;
const uint8_t kTestBinShifts = 64 - kTestBinBits;

template <typename VALUE>
void test_collisions(bool sequential) {
  std::set<HashValue> hashes;
  std::vector<uint32_t> bin_counts(1U << kTestBinBits, 0U);
  assorted::UniformRandom rnd(12345ULL);
  uint64_t hash_collisions = 0;
  const char* name = (sequential ? "seq" : "rnd");
  for (uint64_t i = 0; i < kCollisionReps; ++i) {
    HashValue hash;
    if (sequential) {
      hash = hashinate<VALUE>(i);
    } else {
      hash = hashinate<VALUE>(rnd.next_uint64());
    }
    if (hashes.find(hash) != hashes.end()) {
      ++hash_collisions;
      std::cout << name << " hash collision (" << i << "/" << kCollisionReps << "): hash="
        << hash << std::endl;
    } else {
      hashes.insert(hash);

      HashBin bin = hash >> kTestBinShifts;
      EXPECT_LT(bin, bin_counts.size());
      ++bin_counts[bin];
    }
  }

  double hash_percent = 100.0f * hash_collisions / kCollisionReps;
  std::cout << "Total " << name << " hash collisions: " << hash_collisions << "/"
    << kCollisionReps << " (" << hash_percent << "%)" << std::endl;
  EXPECT_LT(hash_percent, 0.00001f);

  // let's calculate stddev of bin_counts.
  double bin_counts_sum = 0;
  double bin_counts_sq_sum = 0;
  uint32_t max_count = 0;
  for (uint32_t c : bin_counts) {
    bin_counts_sum += c;
    bin_counts_sq_sum += c * c;
    max_count = std::max<uint32_t>(max_count, c);
  }
  double variance =
    (bin_counts_sq_sum - bin_counts_sum * bin_counts_sum / bin_counts.size())
    / (bin_counts.size() - 1);
  double stdev = std::sqrt(variance);
  double average = static_cast<double>(kCollisionReps) / bin_counts.size();
  double stdev_div_ave = stdev / average;
  std::cout << "Total " << name << " bin distribution: max=" << max_count << ", ave=" << average
    << ", stdev=" << stdev << " (" << stdev_div_ave << " [/average])" << std::endl;
  EXPECT_LT(max_count, average * 5);
  EXPECT_LT(stdev_div_ave, 0.5);
}

TEST(HashHashinateTest, SequentialCollisions64) { test_collisions<uint64_t>(true); }
TEST(HashHashinateTest, RandomCollisions64) { test_collisions<uint64_t>(false); }
TEST(HashHashinateTest, SequentialCollisions32) { test_collisions<uint32_t>(true); }
TEST(HashHashinateTest, RandomCollisions32) { test_collisions<uint32_t>(false); }

const uint32_t kBloomInputs = 50;
#ifndef NDEBUG
const uint64_t kBloomReps = 1 << 16;
#else  // NDEBUG
const uint64_t kBloomReps = 1 << 20;
#endif  // NDEBUG

template <typename VALUE>
void test_bloom(bool sequential) {
  DataPageBloomFilter bloom;
  bloom.clear();

  assorted::UniformRandom rnd(12345ULL);
  std::set<VALUE> values;
  for (uint32_t i = 0; i < kBloomInputs; ++i) {
    VALUE v;
    if (sequential) {
      v = i;
    } else {
      v = rnd.next_uint64();
    }
    HashValue hash = hashinate<VALUE>(v);
    bloom.add(DataPageBloomFilter::extract_fingerprint(hash));
    values.insert(v);
  }

  // first, confirm that all correct ones match (no false negatives)
  for (VALUE v : values) {
    HashValue hash = hashinate<VALUE>(v);
    BloomFilterFingerprint fingerprint = DataPageBloomFilter::extract_fingerprint(hash);
    EXPECT_TRUE(bloom.contains(fingerprint));
  }

  // second, test the ones that are not in the set
  uint64_t false_positives = 0;
  const char* name = (sequential ? "seq" : "rnd");
  for (uint64_t i = 0; i < kBloomReps; ++i) {
    VALUE v;
    if (sequential) {
      v = i + kBloomInputs;
    } else {
      while (true) {
        v = rnd.next_uint64();
        if (values.find(v) == values.end()) {
          break;
        }
      }
    }
    ASSERT_ND(values.find(v) == values.end());

    HashValue hash = hashinate<VALUE>(v);
    BloomFilterFingerprint fingerprint = DataPageBloomFilter::extract_fingerprint(hash);
    if (bloom.contains(fingerprint)) {
      ++false_positives;
    }
  }

  double percent = 100.0f * false_positives / kBloomReps;
  std::cout << "Total " << name << " bloom filter false positives: " << false_positives << "/"
    << kBloomReps << " (" << percent << "%)" << std::endl;
  // 512 entries, 50 inputs, 3 hash functions... should be less than 2% false positives, at most 3%
  EXPECT_LT(percent, 3.0f);
}

TEST(HashHashinateTest, SequentialBloomFilter64) { test_bloom<uint64_t>(true); }
TEST(HashHashinateTest, RandomBloomFilter64) { test_bloom<uint64_t>(false); }
TEST(HashHashinateTest, SequentialBloomFilter32) { test_bloom<uint32_t>(true); }
TEST(HashHashinateTest, RandomBloomFilter32) { test_bloom<uint32_t>(false); }


}  // namespace hash
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashHashinateTest, foedus.storage.hash);
