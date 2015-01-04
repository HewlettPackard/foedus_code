/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <cstring>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
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
DEFINE_TEST_CASE_PACKAGE(MasstreePeekTest, foedus.storage.masstree);

const uint32_t kRecords = 1 << 12;
const char* kTableName = "test";
KeySlice nm(uint64_t key) { return normalize_primitive<uint64_t>(key); }

ErrorStack populate_task(const proc::ProcArguments& args, bool two_layers) {
  thread::Thread* context = args.context_;
  MasstreeStorage storage(context->get_engine(), kTableName);
  EXPECT_TRUE(storage.exists());
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;
  bool first_try = true;
  while (true) {
    COERCE_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
    for (uint64_t i = 0; i < kRecords; ++i) {
      if (two_layers) {
        char key[16];
        // only two 2nd-layer. 0 and 10.
        assorted::write_bigendian<uint64_t>(i % 2 == 0 ? 0 : 10, key);
        assorted::write_bigendian<uint64_t>(i, key + 8);
        WRAP_ERROR_CODE(storage.insert_record(context, key, sizeof(key), &i, sizeof(i)));
      } else {
        WRAP_ERROR_CODE(storage.insert_record_normalized(context, nm(i), &i, sizeof(i)));
      }
    }
    if (first_try && two_layers) {
      // currently we don't track moved record to next layer. so we need to reserve records first.
      WRAP_ERROR_CODE(xct_manager->abort_xct(context));
      first_try = false;
    } else {
      WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
      break;
    }
  }
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  COERCE_ERROR(storage.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}
ErrorStack populate_task_one_layer(const proc::ProcArguments& args) {
  return populate_task(args, false);
}
ErrorStack populate_task_two_layers(const proc::ProcArguments& args) {
  return populate_task(args, true);
}

const uint32_t kCapacity = 1 << 12;

void assert_non_empty_boundaries(const MasstreeStorage::PeekBoundariesArguments& args) {
  uint32_t found = *args.found_boundary_count_;
  const KeySlice* boundaries = args.found_boundaries_;
  EXPECT_GT(found, 0) << "from=" << args.from_ << ",to=" << args.to_;
  for (uint32_t i = 0; i < found; ++i) {
    EXPECT_GT(boundaries[i], args.from_) << i;
    EXPECT_LT(boundaries[i], args.to_) << i;
    if (i > 0) {
      EXPECT_GT(boundaries[i], boundaries[i - 1]) << i;
    }
  }
  // std::cout << found << " boundaries between " << args.from_ << " to " << args.to_ << std::endl;
}

ErrorStack verify_one_layer(Engine *engine) {
  MasstreeStorage storage(engine, kTableName);
  KeySlice boundaries[kCapacity];
  std::memset(boundaries, 0, sizeof(boundaries));
  uint32_t found = 0;
  MasstreeStorage::PeekBoundariesArguments args;

  args = { nullptr, 0, kCapacity, nm(0), nm(kRecords / 2), boundaries, &found };
  WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
  assert_non_empty_boundaries(args);

  found = 0;
  args.from_ = nm(kRecords / 2);
  args.to_ = nm((kRecords / 2) + 1);
  WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
  EXPECT_EQ(found, 0);

  found = 0;
  args.from_ = nm(kRecords / 2);
  args.to_ = nm(kRecords);
  WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
  assert_non_empty_boundaries(args);

  found = 0;
  args.from_ = nm(0);
  args.to_ = nm(kRecords * 3);
  WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
  assert_non_empty_boundaries(args);

  return kRetOk;
}

ErrorStack verify_two_layers(Engine *engine) {
  MasstreeStorage storage(engine, kTableName);
  KeySlice boundaries[kCapacity];
  std::memset(boundaries, 0, sizeof(boundaries));
  uint32_t found = 0;
  MasstreeStorage::PeekBoundariesArguments args;

  // rep-0: prefix=0, rep-1: prefix-10, rep-2: no-matches (prefix-5)
  KeySlice prefixes[3] = {0, 10, 5};
  for (uint16_t rep = 0; rep < 3U; ++rep) {
    KeySlice prefix = prefixes[rep];
    SCOPED_TRACE(testing::Message() << "Two-layers, prefix=" << prefix);
    args = { &prefix, 1, kCapacity, nm(0), nm(kRecords / 2), boundaries, &found };
    WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
    if (rep == 2U)  {
      EXPECT_EQ(found, 0);
    } else {
      assert_non_empty_boundaries(args);
    }

    found = 0;
    args.from_ = nm(kRecords / 2);
    args.to_ = nm((kRecords / 2) + 1);
    WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
    EXPECT_EQ(found, 0);

    found = 0;
    args.from_ = nm(kRecords / 2);
    args.to_ = nm(kRecords);
    WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
    if (rep == 2U)  {
      EXPECT_EQ(found, 0);
    } else {
      assert_non_empty_boundaries(args);
    }

    found = 0;
    args.from_ = nm(0);
    args.to_ = nm(kRecords * 3);
    WRAP_ERROR_CODE(storage.peek_volatile_page_boundaries(engine, args));
    if (rep == 2U)  {
      EXPECT_EQ(found, 0);
    } else {
      assert_non_empty_boundaries(args);
    }
  }

  return kRetOk;
}

void execute_test(bool two_layers) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  proc::Proc populate = two_layers ? populate_task_two_layers : populate_task_one_layer;
  engine.get_proc_manager()->pre_register("populate", populate);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage out;
    Epoch commit_epoch;
    MasstreeMetadata meta(kTableName);
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("populate"));
    if (two_layers) {
      COERCE_ERROR(verify_two_layers(&engine));
    } else {
      COERCE_ERROR(verify_one_layer(&engine));
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreePeekTest, OneLayer) { execute_test(false); }
TEST(MasstreePeekTest, TwoLayers) { execute_test(true); }

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreePeekTest, foedus.storage.masstree);
