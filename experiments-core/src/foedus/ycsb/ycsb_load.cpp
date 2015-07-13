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
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/ycsb/ycsb.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace ycsb {

ErrorStack ycsb_load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.output_buffer_size_ < sizeof(std::pair<uint32_t, uint32_t>)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  *args.output_used_ = sizeof(std::pair<uint32_t, uint32_t>);
  const uint32_t *nr_workers = reinterpret_cast<const uint32_t*>(args.input_buffer_);
  std::pair<uint32_t, uint32_t> *start_key_pair =
    reinterpret_cast<std::pair<uint32_t, uint32_t>* >(args.output_buffer_);
  YcsbLoadTask task;
  return task.run(context, *nr_workers, start_key_pair);
}

ErrorStack YcsbLoadTask::run(thread::Thread* context,
  const uint32_t nr_workers, std::pair<uint32_t, uint32_t> *start_key_pair)
{
  Engine* engine = context->get_engine();

  // Create an empty table
  Epoch ep;
  // TODO: adjust fill factor by workload (A...E)
  storage::masstree::MasstreeMetadata meta("ycsb_user_table", 100);

  // "keep volatile pages for now"
  meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0xFFFFFFFFU;
  meta.snapshot_drop_volatile_pages_btree_levels_ = 0;
  meta.snapshot_drop_volatile_pages_layer_threshold_ = 8;
  CHECK_ERROR(engine->get_storage_manager()->create_storage(&meta, &ep));
  LOG(INFO) << "[YCSB] Created user table";

  auto user_table = engine->get_storage_manager()->get_masstree("ycsb_user_table");
  auto* xct_manager = engine->get_xct_manager();

  LOG(INFO) << "[YCSB] Will insert " << kInitialUserTableSize << " records to user table";

  // Now populate the table, round-robin for each worker id (as the high bits).
  auto remaining_inserts = kInitialUserTableSize;
  uint32_t high = 0, low = 0;
  YcsbKey key;
  YcsbRecord r('a');

  debugging::StopWatch watch;
  while (remaining_inserts) {
    COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (high = 0; high < nr_workers and remaining_inserts--; high++) {
      key.build(high, low);
      COERCE_ERROR_CODE(user_table.insert_record(context, key.ptr(), key.size(), &r, sizeof(r)));
    }
    Epoch commit_epoch;
    COERCE_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    //COERCE_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
    low++;
  }
  watch.stop();

  // Note we did a low++ in the while loop above, so workers with id < high
  // actually had low bits=low-1, the rest had low-2. Because the worker will
  // start with local_key_counter (instead of ++), the initial values for
  // workers' local_key_counter will be low for those with id < high, and
  // low-1 for those with id >= high. Both high and low values are passed out
  // through start_key_pair.
  start_key_pair->first = high;
  start_key_pair->second = low;
  LOG(INFO) << "Start key: high=" << start_key_pair->first << " low=" << start_key_pair->second;
  ASSERT_ND(remaining_inserts == 0);
  LOG(INFO) << "[YCSB] Finished loading "
    << kInitialUserTableSize
    << " records in user table in "
    << watch.elapsed_sec() << "s";
  return kRetOk;
}

}  // namespace ycsb
}  // namespace foedus
