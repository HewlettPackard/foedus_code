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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <sstream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/sequential/sequential_cursor.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/thread/impersonate_session.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_manager.hpp"

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

const uint32_t kRecordsPerXct = 1 << 6;
const uint32_t kXctsPerCore = 1 << 6;
const uint32_t kMaxXctsPerEpoch = 1 << 3;
const uint64_t kRecordsPerCore = kRecordsPerXct * kXctsPerCore;
const uint16_t kCoresPerNode = 1U;
const uint64_t kRecordsPerNode = kRecordsPerCore * kCoresPerNode;
const char*    kStorageName = "test";

/**
 * For the verification, each thread share their status in this struct.
 */
struct PerThreadData {
  /** The commit-epoch of each transaction */
  Epoch xct_epochs_[kXctsPerCore];
  uint32_t aborts_in_cursor_;
};

struct SharedData {
  PerThreadData* get_per_thread_data(uint16_t node, uint16_t core_ordinal) {
    ASSERT_ND(node < node_count_);
    ASSERT_ND(core_ordinal < kCoresPerNode);
    uint16_t index = node * kCoresPerNode + core_ordinal;
    return per_thread_data_ + index;
  }

  uint64_t calculate_correct_record_count(
    Epoch from_epoch,
    Epoch to_epoch,
    int32_t node_filter) const {
    uint64_t result = 0;
    for (uint16_t node = 0; node < node_count_; ++node) {
      if (node_filter >= 0 && node != node_filter) {
        continue;
      }
      for (uint16_t core = 0; core < kCoresPerNode; ++core) {
        for (uint32_t x = 0; x < kXctsPerCore; ++x) {
          Epoch epoch = per_thread_data_[node * kCoresPerNode + core].xct_epochs_[x];
          if (epoch >= from_epoch && epoch < to_epoch) {
            ++result;
          }
        }
      }
    }
    ASSERT_ND(result * kRecordsPerXct <= total_records_);
    return result * kRecordsPerXct;
  }

  PerThreadData per_thread_data_[4];  // at most 2-nodes * 2-cores
  uint64_t total_records_;
  uint16_t node_count_;
  bool has_volatile_;
  bool has_snapshot_;
  Epoch snapshot_epoch_;
  Epoch begin_epoch_;
  Epoch end_epoch_;

  /**
   * Used in the loader.
   * the thread that made it == node_count*kCoresPerNode should wake up the epoch chime
   */
  std::atomic<uint32_t> epoch_waiting_count_;
};

ErrorStack load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  SequentialStorage sequential(context->get_engine(), kStorageName);
  EXPECT_TRUE(sequential.exists());

  SharedData* shared_data = reinterpret_cast<SharedData*>(
    context->get_engine()->get_memory_manager()->get_shared_user_memory());
  uint16_t node = thread::decompose_numa_node(context->get_thread_id());
  uint16_t core_ordinal = thread::decompose_numa_local_ordinal(context->get_thread_id());
  PerThreadData* my_data = shared_data->get_per_thread_data(node, core_ordinal);

  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  const Epoch first_epoch = xct_manager->get_current_global_epoch();

  Epoch cur_epoch = first_epoch;
  uint32_t cur_epoch_xcts = 0;
  LOG(INFO) << "Thread-" << context->get_thread_id() << " stated loading data. cur_epoch="
    << cur_epoch;

  memory::AlignedMemory read_buffer(
    1U << 16,
    1U << 12,
    memory::AlignedMemory::kNumaAllocOnnode,
    context->get_numa_node());

  const uint64_t offset = node * kRecordsPerNode + core_ordinal * kRecordsPerCore;
  uint64_t data;
  Epoch last_epoch;
  for (uint32_t x = 0; x < kXctsPerCore; ++x) {
    // LOG(INFO) << "xct=" << x
    //  << context->get_engine()->get_memory_manager()->dump_free_memory_stat();

    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (uint32_t t = 0; t < kRecordsPerXct; ++t) {
      data = offset + x * kRecordsPerXct + t;
      ASSERT_ND(data < shared_data->total_records_);
      WRAP_ERROR_CODE(sequential.append_record(context, &data, sizeof(data)));
    }
    Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    my_data->xct_epochs_[x] = commit_epoch;
    ASSERT_ND(commit_epoch >= cur_epoch);
    last_epoch = commit_epoch;

    // just as a random test, run a cursor now in a racy setting
    // this might or might not abort because of page-set/read-set.
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    SequentialCursor cursor(
      context,
      sequential,
      read_buffer.get_block(),
      read_buffer.get_size(),
      SequentialCursor::kNodeFirstMode,
      first_epoch,
      last_epoch.one_more().one_more());  // so that we touch even unsafe epochs
    SequentialRecordIterator it;
    while (cursor.is_valid()) {
      WRAP_ERROR_CODE(cursor.next_batch(&it));
      while (it.is_valid()) {
        Epoch record_epoch = it.get_cur_record_epoch();
        EXPECT_TRUE(record_epoch.is_valid());
        it.next();
      }
    }
    ErrorCode code = xct_manager->precommit_xct(context, &commit_epoch);
    EXPECT_TRUE(code == kErrorCodeOk || code == kErrorCodeXctRaceAbort);
    ASSERT_ND(!context->get_current_xct().is_active());
    if (code == kErrorCodeXctRaceAbort) {
      // this will probably happen a few times during the test, but might not happen.
      LOG(INFO) << "Thread-" << context->get_thread_id() << " observed aborts in a"
        << " racy cursor execution. This is expected.";
      ++my_data->aborts_in_cursor_;
    }

    // Make sure we output records in several epochs. So, wait a bit.
    if (commit_epoch > cur_epoch) {
      cur_epoch = commit_epoch;
      cur_epoch_xcts = 1;
    } else {
      ++cur_epoch_xcts;
      if (cur_epoch_xcts >= kMaxXctsPerEpoch) {
        LOG(INFO) << "Thread-" << context->get_thread_id() << " finished " << x << " xcts."
          << " Now waiting for next epoch.. cur_epoch=" << cur_epoch;
        Epoch next_epoch = cur_epoch.one_more();
        uint32_t new_count = shared_data->epoch_waiting_count_.fetch_add(1U) + 1U;
        if (new_count == shared_data->node_count_ * kCoresPerNode) {
          LOG(INFO) << "Okay okay, i'll wake up the epoch chime";
          xct_manager->advance_current_global_epoch();
        } else {
          xct_manager->wait_for_current_global_epoch(next_epoch);
        }
        --shared_data->epoch_waiting_count_;
      }
    }
  }

  WRAP_ERROR_CODE(xct_manager->wait_for_commit(last_epoch));
  LOG(INFO) << "Thread-" << context->get_thread_id() << " loaded data. cur_epoch=" << cur_epoch;
  return foedus::kRetOk;
}

ErrorStack scan_task_impl(
  const proc::ProcArguments& args,
  Epoch from_epoch,
  Epoch to_epoch,
  int32_t node_filter) {
  thread::Thread* context = args.context_;
  SequentialStorage sequential(context->get_engine(), kStorageName);
  EXPECT_TRUE(sequential.exists());

  SharedData* shared_data = reinterpret_cast<SharedData*>(
    context->get_engine()->get_memory_manager()->get_shared_user_memory());
  SCOPED_TRACE(testing::Message()
    << shared_data->node_count_ << " nodes"
    << (shared_data->has_snapshot_ ? " has_snapshot" : "")
    << (shared_data->has_volatile_ ? " has_volatile" : "")
    << " from=" << from_epoch << ", to=" << to_epoch << ", node_filter=" << node_filter);

  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  memory::AlignedMemory read_buffer(
    1U << 13,  // deliberately using a small buffer to test page-flip
    1U << 12,
    memory::AlignedMemory::kNumaAllocOnnode,
    context->get_numa_node());

  uint16_t node_count = shared_data->node_count_;

  uint64_t record_count = 0;
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  SequentialCursor cursor(
    context,
    sequential,
    read_buffer.get_block(),
    read_buffer.get_size(),
    SequentialCursor::kNodeFirstMode,
    from_epoch,
    to_epoch,
    node_filter);
  std::vector<bool> observed;
  observed.assign(shared_data->total_records_, false);
  ASSERT_ND(observed.size() == shared_data->total_records_);
  while (cursor.is_valid()) {
    SequentialRecordIterator it;
    if (from_epoch.value() == 11U && record_count > 420) {
      LOG(INFO) << "aa";
    }
    WRAP_ERROR_CODE(cursor.next_batch(&it));

    const SequentialPage* page = reinterpret_cast<const SequentialPage*>(it.get_raw_batch());
    uint16_t node = 0;
    Epoch single_epoch;
    if (it.is_valid()) {
      if (page->header().snapshot_) {
        ASSERT_ND(shared_data->has_snapshot_);
        node = extract_numa_node_from_snapshot_pointer(page->header().page_id_);
      } else {
        ASSERT_ND(shared_data->has_volatile_);
        VolatilePagePointer page_id;
        page_id.word = page->header().page_id_;
        node = page_id.components.numa_node;
        EXPECT_GT(page_id.components.offset, 0);

        // volatile sequential page is guaranteed to contain only one epoch
        single_epoch = page->get_first_record_epoch();
        EXPECT_TRUE(single_epoch.is_valid());
        EXPECT_GE(single_epoch, from_epoch);
        EXPECT_LT(single_epoch, to_epoch);
      }
      page->assert_consistent();
    }
    EXPECT_LT(node, node_count);

    while (it.is_valid()) {
      Epoch record_epoch = it.get_cur_record_epoch();
      if (single_epoch.is_valid()) {
        EXPECT_EQ(single_epoch, record_epoch);
      }
      EXPECT_FALSE(it.get_cur_record_owner_id()->lock_.is_keylocked());
      EXPECT_FALSE(it.get_cur_record_owner_id()->lock_.is_rangelocked());
      const char* payload = it.get_cur_record_raw();
      uint64_t data = *reinterpret_cast<const uint64_t*>(payload);
      ASSERT_ND(data < observed.size());
      uint64_t record_node = data / kRecordsPerNode;
      EXPECT_EQ(node, record_node) << data;
      uint64_t core_ordinal = (data % kRecordsPerNode) / kRecordsPerCore;
      EXPECT_LT(core_ordinal, kCoresPerNode) << data;
      if (node_filter >= 0) {
        EXPECT_EQ(node_filter, record_node) << data;
      }
      EXPECT_TRUE(record_epoch.is_valid()) << data;
      EXPECT_GE(record_epoch, from_epoch) << data;
      EXPECT_LT(record_epoch, to_epoch) << data;

      uint64_t per_core_index = data % kRecordsPerCore;
      uint64_t xct = per_core_index / kRecordsPerXct;
      PerThreadData* per_thread_data = shared_data->get_per_thread_data(record_node, core_ordinal);
      Epoch correct_epoch = per_thread_data->xct_epochs_[xct];
      EXPECT_EQ(correct_epoch, record_epoch) << data;

      EXPECT_FALSE(observed[data]) << data;
      observed[data] = true;
      ++record_count;
      it.next();
    }
  }

  uint64_t correct_count
    = shared_data->calculate_correct_record_count(from_epoch, to_epoch, node_filter);
  if (from_epoch == shared_data->begin_epoch_
    && to_epoch == shared_data->end_epoch_
    && (node_filter == -1 || node_count == 1U)) {
    // full scan case
    EXPECT_EQ(node_count * kCoresPerNode * kXctsPerCore * kRecordsPerXct, correct_count);
  }
  EXPECT_EQ(correct_count, record_count);

  // Above tests can detect records that shouldn't be returned.
  // Now look for records that should have been returned.
  // Total count alone isn't enough informative.
  for (uint32_t node = 0; node < shared_data->node_count_; ++node) {
    if (node_filter >= 0 && node != static_cast<uint32_t>(node_filter)) {
      continue;
    }
    for (uint32_t core = 0; core < kCoresPerNode; ++core) {
      PerThreadData* per_thread_data = shared_data->get_per_thread_data(node, core);
      for (uint32_t x = 0; x < kXctsPerCore; ++x) {
        Epoch epoch = per_thread_data->xct_epochs_[x];
        if (epoch < from_epoch || epoch >= to_epoch) {
          continue;
        }
        uint64_t offset = node * kRecordsPerNode + core * kRecordsPerCore + x * kRecordsPerXct;
        for (uint32_t record = offset; record < offset + kRecordsPerXct; ++record) {
          EXPECT_TRUE(observed[record]) << "record=" << record
            << "(node=" << node << ", core=" << core << ", xct=" << x << ")";
        }
      }
    }
  }


  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  return foedus::kRetOk;
}

ErrorStack scan_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;

  SharedData* shared_data = reinterpret_cast<SharedData*>(
    context->get_engine()->get_memory_manager()->get_shared_user_memory());
  LOG(INFO) << "Stated scanning";

  // First, test full-scan without unsafe epochs
  // Because we made everything durable, this actually returns all records
  CHECK_ERROR(scan_task_impl(args, shared_data->begin_epoch_, shared_data->end_epoch_, -1));

  LOG(INFO) << "full scan test done";

  // Second, test partial-scan with epochs, including unsafe epochs.
  const uint32_t kEpochStep = 2;
  Epoch initial_from = shared_data->begin_epoch_.one_less();  // -1 to test empty-epoch case.
  for (Epoch::EpochInteger from = initial_from.value();
        from < shared_data->end_epoch_.value();
        from += kEpochStep) {
    Epoch from_epoch(from);
    Epoch to_epoch(from + kEpochStep);
    CHECK_ERROR(scan_task_impl(args, from_epoch, to_epoch, -1));
  }

  LOG(INFO) << "partial scan test done";

  // Finally, partial-scan with node filter.
  for (Epoch::EpochInteger from = initial_from.value();
        from < shared_data->end_epoch_.value();
        from += kEpochStep) {
    Epoch from_epoch(from);
    Epoch to_epoch(from + kEpochStep);
    for (uint16_t node = 0; node < shared_data->node_count_; ++node) {
      CHECK_ERROR(scan_task_impl(args, from_epoch, to_epoch, node));
    }
  }

  LOG(INFO) << "all tests done";
  return foedus::kRetOk;
}

void test_cursor(
  bool has_volatile,
  bool has_snapshot,
  bool multi_node) {
  EngineOptions options = get_tiny_options();
  const uint16_t kRecordsPerPageConservative = 8;
  uint32_t pages_conservative
    = kRecordsPerXct * kXctsPerCore / kRecordsPerPageConservative;
  options.memory_.page_pool_size_mb_per_node_
    = kCoresPerNode * assorted::int_div_ceil(pages_conservative * kPageSize, 1ULL << 20);
  options.thread_.thread_count_per_group_ = kCoresPerNode;
  uint16_t node_count = 1;
  if (multi_node) {
    node_count = 2;
  }
  options.thread_.group_count_ = node_count;

  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("load_task", load_task));
  engine.get_proc_manager()->pre_register(proc::ProcAndName("scan_task", scan_task));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    {
      SequentialMetadata meta(kStorageName);
      SequentialStorage storage;
      Epoch epoch;
      COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta, &storage, &epoch));
      EXPECT_TRUE(storage.exists());

      SharedData* shared_data = reinterpret_cast<SharedData*>(
        engine.get_memory_manager()->get_shared_user_memory());
      ASSERT_ND(engine.get_memory_manager()->get_shared_user_memory_size() >= sizeof(SharedData));
      std::memset(shared_data, 0, sizeof(SharedData));
      shared_data->total_records_ = node_count * kRecordsPerNode;
      shared_data->node_count_ = node_count;
      shared_data->has_volatile_ = has_volatile;
      shared_data->has_snapshot_ = has_snapshot;

      xct::XctManager* xct_manager = engine.get_xct_manager();
      const Epoch begin_epoch = xct_manager->get_current_global_epoch();
      shared_data->begin_epoch_ = begin_epoch;

      // First, load the data in each core.
      std::vector< thread::ImpersonateSession > sessions;
      for (uint16_t node = 0; node < node_count; ++node) {
        for (uint16_t core = 0; core < kCoresPerNode; ++core) {
          thread::ThreadId thread_id = thread::compose_thread_id(node, core);
          thread::ImpersonateSession session;
          bool impersonated = engine.get_thread_pool()->impersonate_on_numa_core(
            thread_id,
            "load_task",
            nullptr,
            0,
            &session);
          EXPECT_TRUE(impersonated);
          sessions.emplace_back(std::move(session));
        }
      }

      LOG(INFO) << "Launched loaders. waitnig...";
      for (thread::ImpersonateSession& s : sessions) {
        COERCE_ERROR(s.get_result());
        s.release();
      }
      LOG(INFO) << "Loaders all done.";

      engine.get_xct_manager()->advance_current_global_epoch();

      const Epoch end_epoch = xct_manager->get_current_global_epoch();
      shared_data->end_epoch_ = end_epoch;

      uint64_t all_records = shared_data->calculate_correct_record_count(
        shared_data->begin_epoch_,
        shared_data->end_epoch_,
        -1);
      EXPECT_EQ(shared_data->total_records_, all_records);
      for (uint16_t node = 0; node < node_count; ++node) {
        uint64_t node_records = shared_data->calculate_correct_record_count(
          shared_data->begin_epoch_,
          shared_data->end_epoch_,
          node);
        EXPECT_EQ(shared_data->total_records_ / node_count, node_records);
      }

      // Let's do sanity check on committed epochs. Also, we need to pick
      // epochs up to which we take snapshot.
      for (uint16_t node = 0; node < node_count; ++node) {
        for (uint16_t core = 0; core < kCoresPerNode; ++core) {
          uint32_t ordinal = node * kCoresPerNode + core;
          const PerThreadData& data = shared_data->per_thread_data_[ordinal];
          for (uint32_t x = 0; x < kXctsPerCore; ++x) {
            Epoch epoch = data.xct_epochs_[x];
            EXPECT_TRUE(epoch.is_valid());
            EXPECT_GE(epoch, begin_epoch);
            EXPECT_LT(epoch, end_epoch);
          }
        }
      }
      Epoch middle_epoch = shared_data->per_thread_data_[0].xct_epochs_[kXctsPerCore / 2];
      LOG(INFO) << "middle_epoch=" << middle_epoch << ", begin_epoch=" << begin_epoch
        << ", end_epoch=" << end_epoch << ", Thread-0's first_epoch="
        << shared_data->per_thread_data_[0].xct_epochs_[0]
        << ", Thread-0's last_epoch="
        << shared_data->per_thread_data_[0].xct_epochs_[kXctsPerCore - 1]
        << ", Thread-0's aborts during racy cursors="
        << shared_data->per_thread_data_[0].aborts_in_cursor_ << "/" << kXctsPerCore;

      // let's take snapshot
      if (has_snapshot) {
        Epoch snapshot_epoch;
        ASSERT_ND(has_volatile || has_snapshot);
        if (!has_volatile) {
          // snapshot everything that is durable
          snapshot_epoch = engine.get_log_manager()->get_durable_global_epoch();
        } else {
          // snapshot only half of them
          snapshot_epoch = middle_epoch;
        }

        engine.get_snapshot_manager()->trigger_snapshot_immediate(true, snapshot_epoch);
        EXPECT_EQ(engine.get_snapshot_manager()->get_snapshot_epoch(), snapshot_epoch);
        shared_data->snapshot_epoch_ = snapshot_epoch;
      }

      // Finally, scan the outcomes!
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("scan_task"));
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(SequentialCursorTest, Volatile1Node) { test_cursor(true, false, false); }
TEST(SequentialCursorTest, Snapshot1Node) { test_cursor(false, true, false); }
TEST(SequentialCursorTest, Both1Node)     { test_cursor(true, true, false); }

TEST(SequentialCursorTest, Volatile2Node) { test_cursor(true, false, true); }
TEST(SequentialCursorTest, Snapshot2Node) { test_cursor(false, true, true); }
TEST(SequentialCursorTest, Both2Node)     { test_cursor(true, true, true); }

}  // namespace sequential
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SequentialCursorTest, foedus.storage.sequential);
