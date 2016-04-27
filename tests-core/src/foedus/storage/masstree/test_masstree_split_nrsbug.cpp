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
#include <map>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/spin_until_impl.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeSplitNrsbugTest, foedus.storage.masstree);

const StorageName kStorageName("test");

/**
 * This testcase tries to specifically reproduce a bug in no-record-split
 * in intermedaite page, which resulted in a non-empty-range intermediate page
 * without any pointer (invalid!)
 */
ErrorStack nrs_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree(context->get_engine(), kStorageName);
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;
  ASSERT_ND(args.input_len_ == sizeof(bool));
  bool reversed = *reinterpret_cast<const bool*>(args.input_buffer_);


  constexpr uint16_t kPayload = 1024;
  char payload[kPayload];
  std::memset(payload, 0, kPayload);

#ifndef NDEBUG
  constexpr uint16_t kBatchCount = 10;
#else  // NDEBUG
  constexpr uint16_t kBatchCount = 1;
#endif  // NDEBUG
  constexpr uint16_t kBatchSize = 100;

  for (uint32_t i = 0; i < kBatchCount; ++i) {
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (uint32_t j = 0; j < kBatchSize; ++j) {
      KeySlice key;
      uint32_t seq = i * kBatchSize + j;
      if (reversed) {
        key = (kBatchCount * kBatchSize - seq - 1) * 2U;
      } else {
        key = seq * 2U;
      }
      WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, payload, kPayload));
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }


  CHECK_ERROR(masstree.debugout_single_thread(context->get_engine()));
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

void test(bool reversed) {
  if (assorted::is_running_on_valgrind()) {
    return;  // takes too long in valgrind
  }
  EngineOptions options = get_tiny_options();
#ifndef NDEBUG
  options.memory_.page_pool_size_mb_per_node_ *= 10;
#endif  // NDEBUG
  Engine engine(options);
  engine.get_proc_manager()->pre_register("nrs_task", nrs_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta(kStorageName);
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous(
      "nrs_task",
      &reversed,
      sizeof(reversed)));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
TEST(MasstreeSplitNrsbugTest, InOrder) { test(false); }
TEST(MasstreeSplitNrsbugTest, Reverse) { test(true); }

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeSplitNrsbugTest, foedus.storage.masstree);
