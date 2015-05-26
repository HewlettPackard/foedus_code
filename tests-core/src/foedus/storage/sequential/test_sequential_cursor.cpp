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

#include <sstream>
#include <string>

#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/storage/sequential/sequential_cursor.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace sequential {
DEFINE_TEST_CASE_PACKAGE(SequentialCursorTest, foedus.storage.sequential);

TEST(SequentialCursorTest, IteratorRawPage) {
  // construct a page by directly manipulating the page.
  memory::AlignedMemory memory;
  memory.alloc(1 << 12, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  SequentialPage* page = reinterpret_cast<SequentialPage*>(memory.get_block());
  page->initialize_snapshot_page(1, to_snapshot_page_pointer(1, 0, 1));

  const uint16_t kRecords = 12;
  const Epoch::EpochInteger kBeginEpoch = 42;
  uint16_t next_epoch_count = 0;
  for (uint16_t i = 0; i < kRecords; ++i) {
    xct::XctId xct_id;
    Epoch::EpochInteger epoch;
    if (i % 3 == 0 || i % 2 == 0) {
      epoch = kBeginEpoch;
    } else {
      epoch = kBeginEpoch + 1;
      ++next_epoch_count;
    }
    xct_id.set(epoch, 123);
    std::stringstream str;
    str << "data_" << i;
    std::string payload = str.str();
    page->append_record_nosync(xct_id, payload.size(), payload.data());
  }

  // so far SequentialPage and SequentialRecordBatch has the exact same layout
  const SequentialRecordBatch* batch = reinterpret_cast<const SequentialRecordBatch*>(page);

  // full-scan test
  {
    SequentialRecordIterator it(batch, Epoch(1), Epoch(100));
    for (uint16_t i = 0; i < kRecords; ++i) {
      EXPECT_TRUE(it.is_valid());
      if (i % 3 == 0 || i % 2 == 0) {
        EXPECT_EQ(Epoch(kBeginEpoch), it.get_cur_record_epoch()) << i;
      } else {
        EXPECT_EQ(Epoch(kBeginEpoch + 1), it.get_cur_record_epoch()) << i;
      }
      std::stringstream str;
      str << "data_" << i;
      std::string payload = str.str();
      uint16_t len = it.get_cur_record_length();
      EXPECT_EQ(payload.size(), len) << i;

      char buffer[128];
      it.copy_cur_record(buffer, 128);
      EXPECT_EQ(payload, std::string(buffer, len)) << i;
      EXPECT_EQ(payload, std::string(it.get_cur_record_raw(), len)) << i;

      it.next();
    }

    EXPECT_FALSE(it.is_valid());
  }

  // read only kBeginEpoch
  {
    SequentialRecordIterator it(batch, Epoch(kBeginEpoch), Epoch(kBeginEpoch + 1));
    for (uint16_t i = 0; i < kRecords; ++i) {
      if (i % 3 == 0 || i % 2 == 0) {
        EXPECT_TRUE(it.is_valid());
        EXPECT_EQ(Epoch(kBeginEpoch), it.get_cur_record_epoch()) << i;
      } else {
        continue;
      }

      std::stringstream str;
      str << "data_" << i;
      std::string payload = str.str();
      uint16_t len = it.get_cur_record_length();
      EXPECT_EQ(payload.size(), len) << i;

      char buffer[128];
      it.copy_cur_record(buffer, 128);
      EXPECT_EQ(payload, std::string(buffer, len)) << i;
      EXPECT_EQ(payload, std::string(it.get_cur_record_raw(), len)) << i;

      it.next();
    }
  }

  // read only kBeginEpoch+1
  {
    SequentialRecordIterator it(batch, Epoch(kBeginEpoch + 1), Epoch(kBeginEpoch + 5));
    for (uint16_t i = 0; i < kRecords; ++i) {
      if (i % 3 == 0 || i % 2 == 0) {
        continue;
      } else {
        EXPECT_TRUE(it.is_valid());
        EXPECT_EQ(Epoch(kBeginEpoch + 1), it.get_cur_record_epoch()) << i;
      }

      std::stringstream str;
      str << "data_" << i;
      std::string payload = str.str();
      uint16_t len = it.get_cur_record_length();
      EXPECT_EQ(payload.size(), len) << i;

      char buffer[128];
      it.copy_cur_record(buffer, 128);
      EXPECT_EQ(payload, std::string(buffer, len)) << i;
      EXPECT_EQ(payload, std::string(it.get_cur_record_raw(), len)) << i;

      it.next();
    }

    EXPECT_FALSE(it.is_valid());
  }

  // read none
  {
    SequentialRecordIterator it(batch, Epoch(kBeginEpoch + 10), Epoch(kBeginEpoch + 15));
    EXPECT_FALSE(it.is_valid());
    it.next();
    EXPECT_FALSE(it.is_valid());
  }
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SequentialCursorTest, foedus.storage.sequential);
