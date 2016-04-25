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
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeCursorNrsbugTest, foedus.storage.masstree);

const StorageName kStorageName("test");

ErrorStack count_all(thread::Thread* context, MasstreeStorage masstree, uint32_t* out) {
  MasstreeCursor cursor(masstree, context);
  WRAP_ERROR_CODE(cursor.open());
  *out = 0;
  while (cursor.is_valid_record()) {
    ++(*out);
    std::cout << "Key=" << assorted::HexString(cursor.get_combined_key()) << std::endl;
    WRAP_ERROR_CODE(cursor.next());
  }
  return kRetOk;
}

ErrorCode insert_customer(thread::Thread* context, const uint64_t* words) {
  MasstreeStorage masstree(context->get_engine(), kStorageName);
  char buffer[16];
  std::memset(buffer, 0, sizeof(buffer));
  for (uint16_t i = 0; i < 2U; ++i) {
    assorted::write_bigendian<uint64_t>(words[i], buffer + (i * sizeof(uint64_t)));
  }
  CHECK_ERROR_CODE(masstree.insert_record(context, buffer, 9));
  return kErrorCodeOk;
}

/**
 * This testcase tries to specifically reproduce a bug observed in TPC-C's
 * Customer's secondary index. In the data loader, when we "fatify" the
 * index, our cursor missed 2 records out of 12000 when no-record-split
 * is triggered.
 * This testcase uses much fewer records to easily reproduce the issue.
 */
ErrorStack nrs_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree(context->get_engine(), kStorageName);
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;


  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  /* The real data in TPC-C. seems like we can reproduce with just 2 words below.
  const uint64_t kStr1[] = {0x0000004241524241ULL, 0x5242415200000000ULL, 0x0000000068677772ULL, 0x636E776D7462676EULL, 0x6900000000000000ULL, 0x00ULL};
  const uint64_t kStr2[] = {0x0000004F55474854ULL, 0x4241524241520000ULL, 0x000000006A6E6963ULL, 0x6A78757169000000ULL, 0x0000000000000000ULL, 0x01ULL};
  const uint64_t kStr3[] = {0x0000014241524241ULL, 0x5242415200000000ULL, 0x00000000756C6777ULL, 0x62716D7565756B6BULL, 0x6762666E00000000ULL, 0x00ULL};
  const uint64_t kStr4[] = {0x0000014F55474854ULL, 0x4241524241520000ULL, 0x000000006C696D74ULL, 0x7576636B6872646EULL, 0x6400000000000000ULL, 0x01ULL};
  */
  const uint64_t kStr1[] = {0x0000004241524241ULL, 0x00ULL};
  const uint64_t kStr2[] = {0x0000004F55474854ULL, 0x01ULL};
  const uint64_t kStr3[] = {0x0000014241524241ULL, 0x00ULL};
  const uint64_t kStr4[] = {0x0000014F55474854ULL, 0x01ULL};
  WRAP_ERROR_CODE(insert_customer(context, kStr1));
  WRAP_ERROR_CODE(insert_customer(context, kStr2));
  WRAP_ERROR_CODE(insert_customer(context, kStr3));
  WRAP_ERROR_CODE(insert_customer(context, kStr4));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));


  CHECK_ERROR(masstree.debugout_single_thread(context->get_engine()));
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  uint32_t count;
  CHECK_ERROR(count_all(context, masstree, &count));
  EXPECT_EQ(4, count);
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.fatify_first_root(context, 6));
  CHECK_ERROR(masstree.verify_single_thread(context));
  CHECK_ERROR(masstree.debugout_single_thread(context->get_engine()));
  CHECK_ERROR(count_all(context, masstree, &count));
  EXPECT_EQ(4, count);
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}


TEST(MasstreeCursorNrsbugTest, Nrs) {
  EngineOptions options = get_tiny_options();
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
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("nrs_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeCursorNrsbugTest, foedus.storage.masstree);
