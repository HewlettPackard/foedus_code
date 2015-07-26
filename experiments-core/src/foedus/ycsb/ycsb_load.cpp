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
  if (args.input_len_ != sizeof(YcsbLoadTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  YcsbLoadTask task;
  const YcsbLoadTask::Inputs* inputs =
    reinterpret_cast<const YcsbLoadTask::Inputs*>(args.input_buffer_);
  return task.run(context, inputs->load_node_, inputs->records_per_thread_);
}

ErrorStack YcsbLoadTask::run(thread::Thread* context, uint16_t node, uint64_t records_per_thread) {
  Engine* engine = context->get_engine();

#ifdef YCSB_HASH_STORAGE
  auto user_table = engine->get_storage_manager()->get_hash("ycsb_user_table");
#else
  auto user_table = engine->get_storage_manager()->get_masstree("ycsb_user_table");
#endif
  auto* xct_manager = engine->get_xct_manager();

  // Now populate the table, round-robin for each worker id (as the high bits) in my group.
  debugging::StopWatch watch;
  auto& options = engine->get_options();
  uint64_t inserted = 0;
  // Insert (equal number of) records on behalf of each worker
  for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ordinal++) {
    auto remaining_inserts = records_per_thread;
    uint32_t high = node * options.thread_.thread_count_per_group_ + ordinal, low = 0;
    YcsbKey key;
    YcsbRecord r('a');
    Epoch commit_epoch;
    while (true) {
      COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      key.build(high, low++);
      COERCE_ERROR_CODE(user_table.insert_record(context, key.ptr(), key.size(), &r, sizeof(r)));
      COERCE_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
      inserted++;
      if (--remaining_inserts == 0) {
        break;
      }
    }
    ASSERT_ND(remaining_inserts == 0);
    COERCE_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  }
  watch.stop();
  ASSERT_ND(inserted == records_per_thread * options.thread_.thread_count_per_group_);
  LOG(INFO) << "[YCSB] Loaded " << inserted << " records in " << watch.elapsed_sec() << "s";
  return kRetOk;
}

}  // namespace ycsb
}  // namespace foedus
