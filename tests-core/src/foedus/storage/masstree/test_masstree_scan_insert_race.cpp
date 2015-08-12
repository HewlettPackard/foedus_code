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
#include <memory>
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
const int kMinCoreCount = 16;
const uint32_t max_scan_length = 10000;

int core_count = kMinCoreCount;
std::unique_ptr<uint32_t[]> local_key_counter;

class YcsbKey {
 private:
  assorted::FixedString<kKeyMaxLength> data_;

 public:
  YcsbKey() {
    data_.append(kKeyPrefix);
  }

  YcsbKey(uint32_t high, uint32_t low) {
    build(high, low);
  }

  YcsbKey& next(uint32_t worker_id, uint32_t* local_key_counter) {
    auto low = ++(*local_key_counter);
    build(worker_id, low);
    return *this;
  }

  void build(uint32_t high_bits, uint32_t low_bits) {
    uint64_t keynum = ((uint64_t)high_bits << 32) | low_bits;
    keynum = (uint64_t)foedus::storage::hash::hashinate(&keynum, sizeof(keynum));
    data_ = kKeyPrefix;
    data_.append(std::to_string(keynum));
  }

  const char *ptr() {
    return data_.data();
  }

  uint32_t size() {
    return data_.length();
  }
};

std::unique_ptr<YcsbKey[]> key_arena;

YcsbKey& next_insert_key(int worker) {
  return key_arena[worker].next(worker, &local_key_counter[worker]);
}

ErrorStack insert_task_long_coerce(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  int64_t remaining_inserts = 10000;
  char data[1000];
  memset(data, 'a', 1000);
  Epoch commit_epoch;
  while (remaining_inserts > 0) {
    for (int i = 0; i < core_count; i++) {
      COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      auto& key = next_insert_key(i);
      COERCE_ERROR_CODE(masstree.insert_record(context, key.ptr(), key.size(), data, 1000));
      COERCE_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    }
    remaining_inserts -= core_count;
  }
  return foedus::kRetOk;
}

ErrorStack insert_scan_task(const proc::ProcArguments& args) {
  uint32_t worker_id = *reinterpret_cast<const int*>(args.input_buffer_);
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  debugging::StopWatch duration;
  assorted::UniformRandom trnd(882746 + worker_id);
  assorted::UniformRandom hrnd(123452388999 + worker_id);
  assorted::UniformRandom lrnd(4584287 + worker_id);
  assorted::UniformRandom crnd(47920 + worker_id);
  // 5 seconds
  while (duration.peek_elapsed_ns() < 5000000 * 1000ULL) {
    // Randomly choose to insert or scan
    char data[1000];
    memset(data, 'a', 1000);
    ErrorCode ret = kErrorCodeOk;
    Epoch commit_epoch;
    if (trnd.uniform_within(1, 100) <= 5) {
      // insert
      COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      YcsbKey& key = next_insert_key(worker_id);
      ret = masstree.insert_record(context, key.ptr(), key.size(), data, 1000);
      if (ret == kErrorCodeOk) {
        ret = xct_manager->precommit_xct(context, &commit_epoch);
      } else {
        COERCE_ERROR_CODE(xct_manager->abort_xct(context));
      }
    } else {
      // Open a cursor to scan
      auto high = hrnd.uniform_within(0, core_count - 1);
      auto cnt = local_key_counter[high];
      if (cnt == 0) {
        // So the guy hasn't even inserted anything and the loader didn't insert
        // in that key space either (because kInitialUserTableSize % nr_workers > 0)
        cnt = 1;
      }
      auto low = lrnd.uniform_within(0, cnt - 1);
      YcsbKey key(high, low);
      COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      storage::masstree::MasstreeCursor cursor(masstree, context);
      ret = cursor.open(key.ptr(), key.size(), nullptr,
        foedus::storage::masstree::MasstreeCursor::kKeyLengthExtremum, true, false, true, false);
      if (ret != kErrorCodeOk) {
        COERCE_ERROR_CODE(xct_manager->abort_xct(context));
      } else {
        uint64_t len = 0;
        int32_t nrecs = crnd.uniform_within(1, max_scan_length);
        while (nrecs-- > 0 && cursor.is_valid_record()) {
          len += cursor.get_payload_length();
          const char *pr = reinterpret_cast<const char *>(cursor.get_payload());
          memcpy(data, pr, 1000);
          cursor.next();
        }
        ret = xct_manager->precommit_xct(context, &commit_epoch);
        if (ret != kErrorCodeOk) {
          WRAP_ERROR_CODE(xct_manager->abort_xct(context));
        }
        ASSERT_ND(len % 1000 == 0);
      }
    }
  }
  return foedus::kRetOk;
}

TEST(MasstreeScanInsertRaceTest, CreateAndInsertAndScan) {
  EngineOptions options = get_big_options();

  // Use kMinCoreCount threads; if one node doesn't fit, try more
  core_count = kMinCoreCount;
  if (options.thread_.thread_count_per_group_ < kMinCoreCount) {
    core_count = options.thread_.thread_count_per_group_;
    int g = 0;
    for (g = 0; g < options.thread_.group_count_ && core_count < kMinCoreCount; g++) {
      core_count += options.thread_.thread_count_per_group_;
    }
    options.thread_.group_count_ = g;
  } else {
    options.thread_.thread_count_per_group_ = kMinCoreCount;
    options.thread_.group_count_ = 1;
  }

  ASSERT_ND(core_count >= kMinCoreCount);
  key_arena = std::unique_ptr<YcsbKey[]>(new YcsbKey[core_count]);
  local_key_counter = std::unique_ptr<uint32_t[]>(new uint32_t[core_count]);
  for (int i = 0; i < core_count; i++) {
    local_key_counter[i] = 0;
  }

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
    uint32_t worker_id = 0;
    for (uint16_t node = 0; node < options.thread_.group_count_; node++) {
      for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ordinal++) {
        thread::ImpersonateSession s;
        bool ret = thread_pool->impersonate_on_numa_node(
          node,
          "insert_scan_task",
          &worker_id,
          sizeof(worker_id),
          &s);
        EXPECT_TRUE(ret);
        ASSERT_ND(ret);
        sessions.emplace_back(std::move(s));
        worker_id++;
      }
    }
    for (auto& s : sessions) {
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
