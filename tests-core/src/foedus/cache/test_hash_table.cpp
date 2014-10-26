/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
DEFINE_TEST_CASE_PACKAGE(HashTableTest, foedus.cache);

TEST(HashTableTest, Instantiate) {
  CacheHashtable hashtable(1234567, 0);
  EXPECT_EQ(1234567U, hashtable.get_physical_buckets());
  EXPECT_GT(1234567U, hashtable.get_logical_buckets());
}

}  // namespace cache
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashTableTest, foedus.cache);
