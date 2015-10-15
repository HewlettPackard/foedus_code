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
#include <stdint.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/assorted/zipfian_random.hpp"

namespace foedus {
namespace assorted {

DEFINE_TEST_CASE_PACKAGE(ZipfianRandomTest, foedus.assorted);

TEST(ZipfianRandomTest, OneMillion) {
  int kItems = 1000000;
  int kBucketSize = 1;
  double thetas[2] = { 0, 0.999999 };  // 0: uniformly random, 0.999: high skew
  for (int i = 0; i < 2; i++) {
    uint64_t max = 0, min = kItems;
    ZipfianRandom rnd(kItems, thetas[i], 777);
    std::vector<uint64_t> histo;
    for (auto j = 0; j < kItems / kBucketSize; j++) {
      histo.push_back(0);
    }
    for (auto j = 0; j < kItems; j++) {
      auto n = rnd.next();
      max = std::max(n, max);
      min = std::min(n, min);
      auto bucket = n / kBucketSize;
      EXPECT_LT(bucket, histo.size());
      histo[bucket]++;
      EXPECT_GT(histo[bucket], 0);
    }

    EXPECT_LT(max, kItems);
    EXPECT_GE(min, 0);

    int items = 0;
    for (uint k = 0; k < histo.size(); k++) {
      items += histo[k];
    }
    EXPECT_EQ(items, kItems);
  }
}

}  // namespace assorted
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(ZipfianRandomTest, foedus.assorted);
