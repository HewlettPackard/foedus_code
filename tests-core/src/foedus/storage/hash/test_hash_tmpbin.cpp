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

#include <iostream>

#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/storage/hash/hash_tmpbin.hpp"

namespace foedus {
namespace storage {
namespace hash {
DEFINE_TEST_CASE_PACKAGE(HashTmpBinTest, foedus.storage.hash);

void test_sequential(bool with_resize) {
  const uint32_t kRecords = 1U << 10;
  uint64_t byte_size = HashTmpBin::kDefaultInitialSize;
  if (with_resize) {
    // this would require resizing
    byte_size = sizeof(HashTmpBin::RecordIndex) * HashTmpBin::kBucketCount + kRecords * 32U;
  }
  HashTmpBin table;
  ASSERT_EQ(kErrorCodeOk, table.create_memory(0, byte_size));
  for (uint32_t i = 0; i < kRecords; ++i) {
    uint32_t key = i;
    uint64_t value = i + 42;
    xct::XctId xct_id;
    xct_id.set(123 + (i / 50), 1 + (i % 50));
    HashValue hash = hashinate(&key, sizeof(key));
    ErrorCode code = table.insert_record(xct_id, &key, sizeof(key), hash, &value, sizeof(value));
    EXPECT_EQ(kErrorCodeOk, code);
  }
  // delete odd-numbered records
  for (uint32_t i = 1; i < kRecords; i += 2U) {
    uint32_t key = i;
    xct::XctId xct_id;
    xct_id.set(500 + (i / 50), 1 + (i % 50));
    xct_id.set_deleted();
    HashValue hash = hashinate(&key, sizeof(key));
    ErrorCode code = table.delete_record(xct_id, &key, sizeof(key), hash);
    EXPECT_EQ(kErrorCodeOk, code);
  }

  // overwrite even-numbered records
  for (uint32_t i = 0; i < kRecords; i += 2U) {
    uint32_t key = i;
    uint64_t value = i + 42 + 100;
    xct::XctId xct_id;
    xct_id.set(600 + (i / 50), 1 + (i % 50));
    HashValue hash = hashinate(&key, sizeof(key));
    ErrorCode code = table.overwrite_record(
      xct_id,
      &key,
      sizeof(key),
      hash,
      &value,
      0,
      sizeof(value));
    EXPECT_EQ(kErrorCodeOk, code);
  }
  // change payload length for 1/4th of records
  for (uint32_t i = 0; i < kRecords; i += 4U) {
    uint32_t key = i;
    uint32_t value = i + 42 + 200;
    xct::XctId xct_id;
    xct_id.set(50000 + (i / 50), 432 + (i % 50));
    HashValue hash = hashinate(&key, sizeof(key));
    ErrorCode code = table.update_record(xct_id, &key, sizeof(key), hash, &value, sizeof(value));
    EXPECT_EQ(kErrorCodeOk, code);
  }
  // out of the deleted records, insert-back half of them with a different value
  for (uint32_t i = 1; i < kRecords; i += 4U) {
    uint32_t key = i;
    uint64_t value = i + 42 + 12345;
    xct::XctId xct_id;
    xct_id.set(66666 + (i / 50), 1 + (i % 50));
    HashValue hash = hashinate(&key, sizeof(key));
    ErrorCode code = table.insert_record(xct_id, &key, sizeof(key), hash, &value, sizeof(value));
    EXPECT_EQ(kErrorCodeOk, code);
  }

  // finally verify the table.
  EXPECT_EQ(kRecords, table.get_records_consumed() - table.get_first_record());
  for (uint32_t index = table.get_first_record(); index < table.get_records_consumed(); ++index) {
    HashTmpBin::Record* record = table.get_record(index);
    EXPECT_EQ(sizeof(uint32_t), record->key_length_);
    EXPECT_EQ(8U, record->aligned_key_length_);
    uint32_t key = *reinterpret_cast<uint32_t*>(record->get_key());
    SCOPED_TRACE(testing::Message() << "key=" << key << ", index=" << index);
    EXPECT_LT(key, kRecords);
    HashValue hash = hashinate(&key, sizeof(key));
    EXPECT_EQ(hash, record->hash_);
    if (key % 2U != 0) {
      EXPECT_EQ(sizeof(uint64_t), record->payload_length_);
      EXPECT_EQ(8U, record->aligned_payload_length_);
      if (key % 4U != 1U) {
        EXPECT_TRUE(record->xct_id_.is_deleted());
        EXPECT_EQ(key + 42U, *reinterpret_cast<uint64_t*>(record->get_payload()));
        EXPECT_EQ(500U + (key / 50U), record->xct_id_.get_epoch_int());
        EXPECT_EQ(1U + (key % 50U), record->xct_id_.get_ordinal());
      } else {
        EXPECT_TRUE(!record->xct_id_.is_deleted());
        EXPECT_EQ(key + 42U + 12345U, *reinterpret_cast<uint64_t*>(record->get_payload()));
        EXPECT_EQ(66666U + (key / 50U), record->xct_id_.get_epoch_int());
        EXPECT_EQ(1U + (key % 50U), record->xct_id_.get_ordinal());
      }
    } else {
      EXPECT_FALSE(record->xct_id_.is_deleted());
      if (key % 4U != 0) {
        EXPECT_EQ(sizeof(uint64_t), record->payload_length_);
        EXPECT_EQ(8U, record->aligned_payload_length_);
        EXPECT_EQ(key + 42U + 100, *reinterpret_cast<uint64_t*>(record->get_payload()));
        EXPECT_EQ(600U + (key / 50U), record->xct_id_.get_epoch_int());
        EXPECT_EQ(1U + (key % 50U), record->xct_id_.get_ordinal());
      } else {
        EXPECT_EQ(sizeof(uint32_t), record->payload_length_);
        EXPECT_EQ(8U, record->aligned_payload_length_);
        EXPECT_EQ(key + 42U + 200U, *reinterpret_cast<uint32_t*>(record->get_payload()));
        EXPECT_EQ(50000U + (key / 50U), record->xct_id_.get_epoch_int());
        EXPECT_EQ(432U + (key % 50U), record->xct_id_.get_ordinal());
      }
    }
  }

  // std::cout << table << std::endl;
}
TEST(HashTmpBinTest, Sequential) { test_sequential(false); }
TEST(HashTmpBinTest, SequentialResize) { test_sequential(true); }

}  // namespace hash
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashTmpBinTest, foedus.storage.hash);
