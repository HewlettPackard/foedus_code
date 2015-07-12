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

ErrorStack ycsb_client_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  const YcsbClientTask::Inputs* inputs
    = reinterpret_cast<const YcsbClientTask::Inputs*>(args.input_buffer_);
  YcsbClientTask task(*inputs);
  return task.run(context);
}

ErrorStack YcsbClientTask::run(thread::Thread* context) {
  context_ = context;
  ASSERT_ND(context_);
  engine_ = context_->get_engine();
  xct_manager_ = engine_->get_xct_manager();
  user_table_ = engine_->get_storage_manager()->get_masstree("ycsb_user_table");
  channel_ = get_channel(engine_);

  // Wait for the driver's order
  channel_->start_rendezvous_.wait();
  LOG(INFO) << "YCSB Client-" << worker_id_
    << " started working on workload " << workload_.desc_ << "!";
  while (not is_stop_requested()) {
    do_xct(workload_);
  }
  return kRetOk;
}

ErrorStack YcsbClientTask::do_xct(YcsbWorkload workload_desc) {
  uint16_t xct_type = rnd_.uniform_within(1, 100);
  // Will need to remember the seed if we want to retry on (system) abort
  //uint64_t seed = rnd_.get_current_seed();
  
  // FIXME: Now everything is uniformly random. Need add Zipfian etc.
  assorted::UniformRandom key_rnd;
  if (xct_type <= workload_desc.read_percent_) {
    return do_read(YcsbKey(key_rnd.uniform_within(1, *&next_key - 1)));
  } else if (xct_type <= workload_desc.update_percent_) {
    return do_update(YcsbKey(key_rnd.uniform_within(1, *&next_key - 1)));
  } else if (xct_type <= workload_desc.insert_percent_) {
    return do_insert(YcsbKey(__sync_fetch_and_add(&next_key, 1)));
  } else {
    auto max_key = *&next_key;
    return do_scan(YcsbKey(key_rnd.uniform_within(1, max_key)),
                   key_rnd.uniform_within(1, max_key));
  }
}

ErrorStack YcsbClientTask::do_read(YcsbKey key) {
  COERCE_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  YcsbRecord r;
  foedus::storage::masstree::PayloadLength payload_len = sizeof(YcsbRecord);
  COERCE_ERROR_CODE(user_table_.get_record(context_, &key, sizeof(key), &r, &payload_len));
  Epoch commit_epoch;
  COERCE_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
  return kRetOk;
}

ErrorStack YcsbClientTask::do_update(YcsbKey key) {
  COERCE_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  YcsbRecord r('b');
  COERCE_ERROR_CODE(user_table_.overwrite_record(context_, &key, sizeof(key), &r, 0, sizeof(r)));
  Epoch commit_epoch;
  COERCE_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
  return kRetOk;
}

ErrorStack YcsbClientTask::do_insert(YcsbKey key) {
  // Guess a key... might already exists
  COERCE_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  YcsbRecord r('a');
  COERCE_ERROR_CODE(user_table_.insert_record(context_, &key, sizeof(key), &r, sizeof(r)));
  Epoch commit_epoch;
  COERCE_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
  return kRetOk;
}

ErrorStack YcsbClientTask::do_scan(YcsbKey start_key, uint64_t nrecs) {
  COERCE_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  storage::masstree::MasstreeCursor cursor(user_table_, context_);
  // vs. open_normalized()?
  COERCE_ERROR_CODE(cursor.open((const char *)&start_key,
    sizeof(start_key), nullptr, foedus::storage::masstree::MasstreeCursor::kKeyLengthExtremum, true, false, true, false));
  while (cursor.is_valid_record()) {
    const YcsbRecord *pr = reinterpret_cast<const YcsbRecord *>(cursor.get_payload());
    YcsbRecord r;
    memcpy(&r, pr, sizeof(r));  // need to do this? like do_tuple_read in Silo/ERMIA.
  }
  Epoch commit_epoch;
  COERCE_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
  return kRetOk;
}

}  // namespace ycsb
}  // namespace foedus
