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

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeScanInsertRaceTest, foedus.storage.masstree);

const int kKeyPrefixLength = 4;  // "user" without \0
const assorted::FixedString<kKeyPrefixLength> kKeyPrefix("user");
const int32_t kKeyMaxLength = kKeyPrefixLength + 32;  // 4 bytes "user" + 32 chars for numbers
const int kCoreCount = 16;
uint32_t local_key_counter[kCoreCount];
const uint32_t max_scan_length = 10000;

class YcsbKey {
 private:
  char data_[kKeyMaxLength];
  uint32_t size_;

 public:
  YcsbKey() {
    size_ = 0;
    memset(data_, '\0', kKeyMaxLength);
    snprintf(data_, kKeyPrefixLength + 1, "%s", kKeyPrefix.data());
  }

  YcsbKey next(uint32_t worker_id, uint32_t* local_key_counter) {
    auto low = ++(*local_key_counter);
    return build(worker_id, low);
  }

  YcsbKey build(uint32_t high_bits, uint32_t low_bits) {
    uint64_t keynum = ((uint64_t)high_bits << 32) | low_bits;
    keynum = (uint64_t)foedus::storage::hash::hashinate(&keynum, sizeof(keynum));
    auto n = snprintf(
      data_ + kKeyPrefixLength, kKeyMaxLength - kKeyPrefixLength + 1, "%lu", keynum);
    ASSERT_ND(n > 0);
    n += kKeyPrefixLength;
    ASSERT_ND(n <= kKeyMaxLength);
    memset(data_ + n, '\0', kKeyMaxLength - n);
    size_ = n;
    return *this;
  }

  const char *ptr() {
    return data_;
  }

  uint32_t size() {
    return size_;
  }
};

YcsbKey key_arena[kCoreCount];

YcsbKey next_insert_key(int worker) {
  return key_arena[worker].next(worker, &local_key_counter[worker]);
}

YcsbKey build_key(uint32_t worker, uint32_t low_bits) {
  return key_arena[worker].build(worker, low_bits);
}

ErrorStack insert_task_long_coerce(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  int64_t remaining_inserts = 10000;
  char data[1000];
  memset(data, 'a', 1000);
  uint64_t inserted = 0;
  Epoch commit_epoch;
  while (remaining_inserts > 0) {
    for (int i = 0; i < kCoreCount; i++) {
      COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      auto key = next_insert_key(i);
      COERCE_ERROR_CODE(masstree.insert_record(context, key.ptr(), key.size(), data, 1000));
      COERCE_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
      inserted++;
    }
    remaining_inserts -= kCoreCount;
  }
  return foedus::kRetOk;
}

ErrorStack insert_scan_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  debugging::StopWatch duration;
  assorted::UniformRandom trnd(882746);
  assorted::UniformRandom hrnd(123452388999);
  assorted::UniformRandom lrnd(4584287);
  assorted::UniformRandom crnd(47920);
  // 4 seconds
  while (duration.peek_elapsed_ns() < 5000000 * 1000ULL) {
    // Randomly choose to insert or scan
    char data[1000];
    memset(data, 'a', 1000);
    ErrorCode ret = kErrorCodeOk;
    Epoch commit_epoch;
    if (trnd.uniform_within(1, 100) <= 5) {
      // insert
      COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      auto high = hrnd.uniform_within(0, kCoreCount - 1);
      YcsbKey key = next_insert_key(high);
      ret = masstree.insert_record(context, key.ptr(), key.size(), data, 1000);
      if (ret == kErrorCodeOk) {
        ret = xct_manager->precommit_xct(context, &commit_epoch);
      } else {
        COERCE_ERROR_CODE(xct_manager->abort_xct(context));
      }
    } else {
      // Open a cursor to scan
      auto high = hrnd.uniform_within(0, kCoreCount - 1);
      auto cnt = local_key_counter[high];
      if (cnt == 0) {
        // So the guy hasn't even inserted anything and the loader didn't insert
        // in that key space either (because kInitialUserTableSize % nr_workers > 0)
        cnt = 1;
      }
      auto low = lrnd.uniform_within(0, cnt - 1);
      YcsbKey key = build_key(high, low);
      COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      storage::masstree::MasstreeCursor cursor(masstree, context);
      COERCE_ERROR_CODE(cursor.open(key.ptr(), key.size(), nullptr,
        foedus::storage::masstree::MasstreeCursor::kKeyLengthExtremum, true, false, true, false));

      int32_t nrecs = crnd.uniform_within(1, max_scan_length);
      while (nrecs-- > 0 && cursor.is_valid_record()) {
        ASSERT_ND(cursor.get_payload_length() == 1000);
        const char *pr = reinterpret_cast<const char *>(cursor.get_payload());
        memcpy(data, pr, 1000);
        cursor.next();
      }
      ret = xct_manager->precommit_xct(context, &commit_epoch);
    }
  }
  return foedus::kRetOk;
}

TEST(MasstreeScanInsertRaceTest, CreateAndInsertAndScan) {
  fs::Path folder("/dev/shm/foedus_ycsb_test");
  if (fs::exists(folder)) {
    fs::remove_all(folder);
  }
  if (!fs::create_directories(folder)) {
    std::cerr << "Couldn't create " << folder << ". err=" << assorted::os_error();
  }

  EngineOptions options;

  fs::Path savepoint_path(folder);
  savepoint_path /= "savepoint.xml";
  options.savepoint_.savepoint_path_.assign(savepoint_path.string());
  ASSERT_ND(!fs::exists(savepoint_path));

  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_ycsb_test/snapshot/node_$NODE$";
  options.log_.folder_path_pattern_ = "/dev/shm/foedus_ycsb_test/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = 1;
  options.log_.flush_at_shutdown_ = false;
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;

  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;

  options.log_.log_buffer_kb_ = 512 << 10;
  options.log_.log_file_size_mb_ = 1 << 15;
  options.memory_.page_pool_size_mb_per_node_ = 4 << 10;

  memset(local_key_counter, '\0', sizeof(uint32_t) * kCoreCount);

  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_scan_task", insert_scan_task);
  engine.get_proc_manager()->pre_register("insert_task_long_coerce", insert_task_long_coerce);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    auto* thread_pool = engine.get_thread_pool();
    COERCE_ERROR(thread_pool->impersonate_synchronous("insert_task_long_coerce"));
    std::vector< thread::ImpersonateSession > sessions;
    for (uint32_t i = 0; i < kCoreCount; i++) {
      thread::ImpersonateSession s;
      bool ret = thread_pool->impersonate_on_numa_core(
        i,
        "insert_scan_task",
        nullptr,
        0,
        &s);
      EXPECT_TRUE(ret);
      ASSERT_ND(ret);
      sessions.emplace_back(std::move(s));
    }
    for (uint32_t i = 0; i < kCoreCount; i++) {
      thread::ImpersonateSession& s = sessions[i];
      ErrorStack result = s.get_result();
      s.release();
    }

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeScanInsertRaceTest, foedus.storage.masstree);
