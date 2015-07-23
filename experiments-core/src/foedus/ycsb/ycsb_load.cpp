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

#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/wait.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "foedus/ycsb/ycsb.hpp"

namespace foedus {
namespace ycsb {

ErrorStack ycsb_load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  *args.output_used_ = sizeof(StartKey);
  StartKey *start_key = reinterpret_cast<StartKey*>(args.output_buffer_);
  YcsbLoadTask task;
  return task.run(context, start_key);
}

ErrorStack YcsbLoadTask::run(thread::Thread* context, StartKey* start_key) {
  Engine* engine = context->get_engine();

  // Create an empty table
  Epoch ep;
  // TODO(tzwang): adjust fill factor by workload (A...E)
#ifdef YCSB_HASH_STORAGE
  storage::hash::HashMetadata meta("ycsb_user_table", 80);
  const float kHashPreferredRecordsPerBin = 5.0;
  // TODO(tzwang): tune the multiplier or make it 1 if there's no inserts
  meta.set_capacity(kInitialUserTableSize * 1.2, kHashPreferredRecordsPerBin);
#else
  storage::masstree::MasstreeMetadata meta("ycsb_user_table", 80);
  meta.snapshot_drop_volatile_pages_btree_levels_ = 0;
  meta.snapshot_drop_volatile_pages_layer_threshold_ = 8;
#endif

  // Keep volatile pages
  meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0xFFFFFFFFU;
  CHECK_ERROR(engine->get_storage_manager()->create_storage(&meta, &ep));
  LOG(INFO) << "[YCSB] Created user table";

#ifdef YCSB_HASH_STORAGE
  auto user_table = engine->get_storage_manager()->get_hash("ycsb_user_table");
#else
  auto user_table = engine->get_storage_manager()->get_masstree("ycsb_user_table");
#endif
  auto* xct_manager = engine->get_xct_manager();

  LOG(INFO) << "[YCSB] Will insert " << kInitialUserTableSize << " records to user table";

  // Now populate the table, round-robin for each worker id (as the high bits).
  auto remaining_inserts = kInitialUserTableSize;
  uint32_t high = 0, low = 0;
  YcsbKey key;
  YcsbRecord r('a');

  debugging::StopWatch watch;
  Epoch commit_epoch;
  auto nr_workers = engine->get_options().thread_.get_total_thread_count();
  while (remaining_inserts) {
    COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (high = 0; high < nr_workers; high++) {
      key.build(high, low);
      COERCE_ERROR_CODE(user_table.insert_record(context, key.ptr(), key.size(), &r, sizeof(r)));
      if (!--remaining_inserts)
        break;
    }
    COERCE_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    low++;
  }
  COERCE_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  watch.stop();

  // Note we did a low++ in the while loop above, so workers with id < high
  // actually had low bits=low-1, the rest had low-2. Because the worker will
  // start with local_key_counter (instead of ++), the initial values for
  // workers' local_key_counter will be low for those with id < high, and
  // low-1 for those with id >= high. Both high and low values are passed out
  // through start_key_pair.
  start_key->high = high;
  start_key->low = low;
  ASSERT_ND(remaining_inserts == 0);
  LOG(INFO) << "[YCSB] Finished loading "
    << kInitialUserTableSize
    << " records in user table in "
    << watch.elapsed_sec() << "s";
  return kRetOk;
}

}  // namespace ycsb
}  // namespace foedus
