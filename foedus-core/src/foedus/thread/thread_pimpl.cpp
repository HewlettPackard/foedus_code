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
#include "foedus/thread/thread_pimpl.hpp"

#include <pthread.h>
#include <sched.h>
#include <glog/logging.h>

#include <atomic>
#include <future>
#include <mutex>
#include <thread>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/cache/cache_hashtable.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace thread {
ThreadPimpl::ThreadPimpl(
  Engine* engine,
  Thread* holder,
  ThreadId id,
  ThreadGlobalOrdinal global_ordinal)
  : engine_(engine),
    holder_(holder),
    id_(id),
    numa_node_(decompose_numa_node(id)),
    global_ordinal_(global_ordinal),
    core_memory_(nullptr),
    node_memory_(nullptr),
    snapshot_cache_hashtable_(nullptr),
    snapshot_page_pool_(nullptr),
    log_buffer_(engine, id),
    current_xct_(engine, id),
    snapshot_file_set_(engine),
    control_block_(nullptr),
    task_input_memory_(nullptr),
    task_output_memory_(nullptr),
    mcs_blocks_(nullptr),
    mcs_rw_blocks_(nullptr) {
}

ErrorStack ThreadPimpl::initialize_once() {
  ASSERT_ND(engine_->get_memory_manager()->is_initialized());

  soc::ThreadMemoryAnchors* anchors
    = engine_->get_soc_manager()->get_shared_memory_repo()->get_thread_memory_anchors(id_);
  control_block_ = anchors->thread_memory_;
  control_block_->initialize(id_);
  task_input_memory_ = anchors->task_input_memory_;
  task_output_memory_ = anchors->task_output_memory_;
  mcs_blocks_ = anchors->mcs_lock_memories_;
  mcs_rw_blocks_ = anchors->mcs_rw_lock_memories_;

  pool_pimpl_ = engine_->get_thread_pool()->get_pimpl();
  node_memory_ = engine_->get_memory_manager()->get_local_memory();
  core_memory_ = node_memory_->get_core_memory(id_);
  if (engine_->get_options().cache_.snapshot_cache_enabled_) {
    snapshot_cache_hashtable_ = node_memory_->get_snapshot_cache_table();
  } else {
    snapshot_cache_hashtable_ = nullptr;
  }
  snapshot_page_pool_ = node_memory_->get_snapshot_pool();
  current_xct_.initialize(core_memory_, &control_block_->mcs_block_current_);
  CHECK_ERROR(snapshot_file_set_.initialize());
  CHECK_ERROR(log_buffer_.initialize());
  global_volatile_page_resolver_
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  local_volatile_page_resolver_ = node_memory_->get_volatile_pool()->get_resolver();

  raw_thread_set_ = false;
  raw_thread_ = std::move(std::thread(&ThreadPimpl::handle_tasks, this));
  raw_thread_set_ = true;
  return kRetOk;
}
ErrorStack ThreadPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  {
    {
      control_block_->status_ = kWaitingForTerminate;
      control_block_->wakeup_cond_.signal();
    }
    LOG(INFO) << "Thread-" << id_ << " requested to terminate";
    if (raw_thread_.joinable()) {
      raw_thread_.join();
    }
    control_block_->status_ = kTerminated;
  }
  {
    // release retired volatile pages. we do this in thread module rather than in memory module
    // because this has to happen before NumaNodeMemory of any node is uninitialized.
    for (uint16_t node = 0; node < engine_->get_soc_count(); ++node) {
      memory::PagePoolOffsetAndEpochChunk* chunk
        = core_memory_->get_retired_volatile_pool_chunk(node);
      memory::PagePool* volatile_pool
        = engine_->get_memory_manager()->get_node_memory(node)->get_volatile_pool();
      if (!chunk->empty()) {
        volatile_pool->release(chunk->size(), chunk);
      }
      ASSERT_ND(chunk->empty());
    }
  }
  batch.emprace_back(snapshot_file_set_.uninitialize());
  batch.emprace_back(log_buffer_.uninitialize());
  core_memory_ = nullptr;
  node_memory_ = nullptr;
  snapshot_cache_hashtable_ = nullptr;
  control_block_->uninitialize();
  return SUMMARIZE_ERROR_BATCH(batch);
}

bool ThreadPimpl::is_stop_requested() const {
  assorted::memory_fence_acquire();
  return control_block_->status_ == kWaitingForTerminate;
}

void ThreadPimpl::handle_tasks() {
  int numa_node = static_cast<int>(decompose_numa_node(id_));
  LOG(INFO) << "Thread-" << id_ << " started running on NUMA node: " << numa_node
    << " control_block address=" << control_block_;
  NumaThreadScope scope(numa_node);
  set_thread_schedule();
  ASSERT_ND(control_block_->status_ == kNotInitialized);
  control_block_->status_ = kWaitingForTask;
  while (!is_stop_requested()) {
    assorted::spinlock_yield();
    {
      uint64_t demand = control_block_->wakeup_cond_.acquire_ticket();
      if (is_stop_requested()) {
        break;
      }
      // these two status are "not urgent".
      if (control_block_->status_ == kWaitingForTask
        || control_block_->status_ == kWaitingForClientRelease) {
        VLOG(0) << "Thread-" << id_ << " sleeping...";
        control_block_->wakeup_cond_.timedwait(demand, 100000ULL, 1U << 16, 1U << 13);
      }
    }
    VLOG(0) << "Thread-" << id_ << " woke up. status=" << control_block_->status_;
    if (control_block_->status_ == kWaitingForExecution) {
      control_block_->output_len_ = 0;
      control_block_->status_ = kRunningTask;
      const proc::ProcName& proc_name = control_block_->proc_name_;
      VLOG(0) << "Thread-" << id_ << " retrieved a task: " << proc_name;
      proc::Proc proc = nullptr;
      ErrorStack result = engine_->get_proc_manager()->get_proc(proc_name, &proc);
      if (result.is_error()) {
        // control_block_->proc_result_
        LOG(ERROR) << "Thread-" << id_ << " couldn't find procedure: " << proc_name;
      } else {
        uint32_t output_used = 0;
        proc::ProcArguments args = {
          engine_,
          holder_,
          task_input_memory_,
          control_block_->input_len_,
          task_output_memory_,
          soc::ThreadMemoryAnchors::kTaskOutputMemorySize,
          &output_used,
        };
        result = proc(args);
        VLOG(0) << "Thread-" << id_ << " run(task) returned. result =" << result
          << ", output_used=" << output_used;
        control_block_->output_len_ = output_used;
      }
      if (result.is_error()) {
        control_block_->proc_result_.from_error_stack(result);
      } else {
        control_block_->proc_result_.clear();
      }
      control_block_->status_ = kWaitingForClientRelease;
      {
        // Wakeup the client if it's waiting.
        control_block_->task_complete_cond_.signal();
      }
      VLOG(0) << "Thread-" << id_ << " finished a task. result =" << result;
    }
  }
  ASSERT_ND(is_stop_requested());
  control_block_->status_ = kTerminated;
  LOG(INFO) << "Thread-" << id_ << " exits";
}
void ThreadPimpl::set_thread_schedule() {
  // this code totally assumes pthread. maybe ifdef to handle Windows.. later!
  SPINLOCK_WHILE(raw_thread_set_ == false) {
    // as the copy to raw_thread_ might happen after the launched thread getting here,
    // we check it and spin. This is mainly to make valgrind happy.
    continue;
  }
  pthread_t handle = static_cast<pthread_t>(raw_thread_.native_handle());
  int policy;
  sched_param param;
  int ret = ::pthread_getschedparam(handle, &policy, &param);
  if (ret) {
    LOG(FATAL) << "WTF. pthread_getschedparam() failed: error=" << assorted::os_error();
  }
  const ThreadOptions& opt = engine_->get_options().thread_;
  // output the following logs just once.
  if (id_ == 0) {
    LOG(INFO) << "The default thread policy=" << policy << ", priority=" << param.__sched_priority;
    if (opt.overwrite_thread_schedule_) {
      LOG(INFO) << "Overwriting thread policy=" << opt.thread_policy_
        << ", priority=" << opt.thread_priority_;
    }
  }
  if (opt.overwrite_thread_schedule_) {
    policy = opt.thread_policy_;
    param.__sched_priority = opt.thread_priority_;
    int priority_max = ::sched_get_priority_max(policy);
    int priority_min = ::sched_get_priority_min(policy);
    if (opt.thread_priority_ > priority_max) {
      LOG(WARNING) << "Thread priority too large. using max value: "
        << opt.thread_priority_ << "->" << priority_max;
      param.__sched_priority = priority_max;
    }
    if (opt.thread_priority_ < priority_min) {
      LOG(WARNING) << "Thread priority too small. using min value: "
        << opt.thread_priority_ << "->" << priority_min;
      param.__sched_priority = priority_min;
    }
    int ret2 = ::pthread_setschedparam(handle, policy, &param);
    if (ret2 == EPERM) {
      // this is a quite common mis-configuratrion, so let's output a friendly error message.
      // also, not fatal. keep running, as this only affects performance.
      LOG(WARNING) << "=========   ATTENTION: Thread-scheduling Permission Error!!!!   ==========\n"
        " pthread_setschedparam() failed due to permission error. This means you have\n"
        " not set appropriate rtprio to limits.conf. You cannot set priority higher than what\n"
        " OS allows. Configure limits.conf (eg. 'kimurhid - rtprio 99') or modify ThreadOptions.\n"
        "=============================               ATTENTION              ======================";
    } else if (ret2) {
      LOG(FATAL) << "WTF pthread_setschedparam() failed: error=" << assorted::os_error();
    }
  }
}

ErrorCode ThreadPimpl::install_a_volatile_page(
  storage::DualPagePointer* pointer,
  storage::Page** installed_page) {
  ASSERT_ND(pointer->snapshot_pointer_ != 0);

  // copy from snapshot version
  storage::Page* snapshot_page;
  CHECK_ERROR_CODE(find_or_read_a_snapshot_page(pointer->snapshot_pointer_, &snapshot_page));
  memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
  if (UNLIKELY(offset == 0)) {
    return kErrorCodeMemoryNoFreePages;
  }
  storage::Page* page = local_volatile_page_resolver_.resolve_offset_newpage(offset);
  std::memcpy(page, snapshot_page, storage::kPageSize);
  // We copied from a snapshot page, so the snapshot flag is on.
  ASSERT_ND(page->get_header().snapshot_);
  page->get_header().snapshot_ = false;  // now it's volatile
  storage::VolatilePagePointer volatile_pointer = storage::combine_volatile_page_pointer(
    numa_node_,
    0,
    0,
    offset);
  page->get_header().page_id_ = volatile_pointer.word;  // and correct page ID

  *installed_page = place_a_new_volatile_page(offset, pointer);
  return kErrorCodeOk;
}

storage::Page* ThreadPimpl::place_a_new_volatile_page(
  memory::PagePoolOffset new_offset,
  storage::DualPagePointer* pointer) {
  while (true) {
    storage::VolatilePagePointer cur_pointer = pointer->volatile_pointer_;
    storage::VolatilePagePointer new_pointer = storage::combine_volatile_page_pointer(
      numa_node_,
      0,
      cur_pointer.components.mod_count + 1,
      new_offset);
    // atomically install it.
    if (cur_pointer.components.offset == 0 &&
      assorted::raw_atomic_compare_exchange_strong<uint64_t>(
        &(pointer->volatile_pointer_.word),
        &(cur_pointer.word),
        new_pointer.word)) {
      // successfully installed
      return local_volatile_page_resolver_.resolve_offset_newpage(new_offset);
      break;
    } else {
      if (cur_pointer.components.offset != 0) {
        // someone else has installed it!
        VLOG(0) << "Interesting. Lost race to install a volatile page. ver-b. Thread-" << id_
          << ", local offset=" << new_offset << " winning offset=" << cur_pointer.components.offset;
        core_memory_->release_free_volatile_page(new_offset);
        storage::Page* placed_page = global_volatile_page_resolver_.resolve_offset(cur_pointer);
        ASSERT_ND(placed_page->get_header().snapshot_ == false);
        return placed_page;
      } else {
        // This is probably a bug, but we might only change mod count for some reason.
        LOG(WARNING) << "Very interesting. Lost race but volatile page not installed. Thread-"
          << id_ << ", local offset=" << new_offset;
        continue;
      }
    }
  }
}

// placed here to allow inlining
ErrorCode Thread::follow_page_pointer(
  storage::VolatilePageInit page_initializer,
  bool tolerate_null_pointer,
  bool will_modify,
  bool take_ptr_set_snapshot,
  storage::DualPagePointer* pointer,
  storage::Page** page,
  const storage::Page* parent,
  uint16_t index_in_parent) {
  return pimpl_->follow_page_pointer(
    page_initializer,
    tolerate_null_pointer,
    will_modify,
    take_ptr_set_snapshot,
    pointer,
    page,
    parent,
    index_in_parent);
}

ErrorCode Thread::follow_page_pointers_for_read_batch(
  uint16_t batch_size,
  storage::VolatilePageInit page_initializer,
  bool tolerate_null_pointer,
  bool take_ptr_set_snapshot,
  storage::DualPagePointer** pointers,
  storage::Page** parents,
  const uint16_t* index_in_parents,
  bool* followed_snapshots,
  storage::Page** out) {
  return pimpl_->follow_page_pointers_for_read_batch(
    batch_size,
    page_initializer,
    tolerate_null_pointer,
    take_ptr_set_snapshot,
    pointers,
    parents,
    index_in_parents,
    followed_snapshots,
    out);
}

ErrorCode Thread::follow_page_pointers_for_write_batch(
  uint16_t batch_size,
  storage::VolatilePageInit page_initializer,
  storage::DualPagePointer** pointers,
  storage::Page** parents,
  const uint16_t* index_in_parents,
  storage::Page** out) {
  return pimpl_->follow_page_pointers_for_write_batch(
    batch_size,
    page_initializer,
    pointers,
    parents,
    index_in_parents,
    out);
}

ErrorCode ThreadPimpl::follow_page_pointer(
  storage::VolatilePageInit page_initializer,
  bool tolerate_null_pointer,
  bool will_modify,
  bool take_ptr_set_snapshot,
  storage::DualPagePointer* pointer,
  storage::Page** page,
  const storage::Page* parent,
  uint16_t index_in_parent) {
  ASSERT_ND(!tolerate_null_pointer || !will_modify);

  storage::VolatilePagePointer volatile_pointer = pointer->volatile_pointer_;
  bool followed_snapshot = false;
  if (pointer->snapshot_pointer_ == 0) {
    if (volatile_pointer.components.offset == 0) {
      // both null, so the page is not created yet.
      if (tolerate_null_pointer) {
        *page = nullptr;
      } else {
        // place an empty new page
        ASSERT_ND(page_initializer);
        // we must not install a new volatile page in snapshot page. We must not hit this case.
        ASSERT_ND(!parent->get_header().snapshot_);
        memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
        if (UNLIKELY(offset == 0)) {
          return kErrorCodeMemoryNoFreePages;
        }
        storage::Page* new_page = local_volatile_page_resolver_.resolve_offset_newpage(offset);
        storage::VolatilePagePointer new_page_id;
        new_page_id.components.numa_node = numa_node_;
        new_page_id.components.offset = offset;
        storage::VolatilePageInitArguments args = {
          holder_,
          new_page_id,
          new_page,
          parent,
          index_in_parent
        };
        page_initializer(args);
        storage::assert_valid_volatile_page(new_page, offset);
        ASSERT_ND(new_page->get_header().snapshot_ == false);

        *page = place_a_new_volatile_page(offset, pointer);
      }
    } else {
      // then we have to follow volatile page anyway
      *page = global_volatile_page_resolver_.resolve_offset(volatile_pointer);
    }
  } else {
    // if there is a snapshot page, we have a few more choices.
    if (volatile_pointer.components.offset != 0) {
      // we have a volatile page, which is guaranteed to be latest
      *page = global_volatile_page_resolver_.resolve_offset(volatile_pointer);
    } else if (will_modify) {
      // we need a volatile page. so construct it from snapshot
      CHECK_ERROR_CODE(install_a_volatile_page(pointer, page));
    } else {
      // otherwise just use snapshot
      CHECK_ERROR_CODE(find_or_read_a_snapshot_page(pointer->snapshot_pointer_, page));
      followed_snapshot = true;
    }
  }
  ASSERT_ND((*page) == nullptr || (followed_snapshot == (*page)->get_header().snapshot_));

  // if we follow a snapshot pointer, remember pointer set
  if (current_xct_.get_isolation_level() == xct::kSerializable) {
    if ((*page == nullptr || followed_snapshot) && take_ptr_set_snapshot) {
      current_xct_.add_to_pointer_set(&pointer->volatile_pointer_, volatile_pointer);
    }
  }
  return kErrorCodeOk;
}

ErrorCode ThreadPimpl::follow_page_pointers_for_read_batch(
  uint16_t batch_size,
  storage::VolatilePageInit page_initializer,
  bool tolerate_null_pointer,
  bool take_ptr_set_snapshot,
  storage::DualPagePointer** pointers,
  storage::Page** parents,
  const uint16_t* index_in_parents,
  bool* followed_snapshots,
  storage::Page** out) {
  ASSERT_ND(tolerate_null_pointer || page_initializer);
  if (batch_size == 0) {
    return kErrorCodeOk;
  } else if (UNLIKELY(batch_size > Thread::kMaxFindPagesBatch)) {
    return kErrorCodeInvalidParameter;
  }

  // this one uses a batched find method for following snapshot pages.
  // some of them might follow volatile pages, so we do it only when at least one snapshot ptr.
  bool has_some_snapshot = false;
  const bool needs_ptr_set
    = take_ptr_set_snapshot && current_xct_.get_isolation_level() == xct::kSerializable;

  // REMINDER: Remember that it might be parents == out. We thus use tmp_out.
  storage::Page* tmp_out[Thread::kMaxFindPagesBatch];
#ifndef NDEBUG
  // fill with garbage for easier debugging
  std::memset(tmp_out, 0xDA, sizeof(tmp_out));
#endif  // NDEBUG

  // collect snapshot page IDs.
  storage::SnapshotPagePointer snapshot_page_ids[Thread::kMaxFindPagesBatch];
  for (uint16_t b = 0; b < batch_size; ++b) {
    snapshot_page_ids[b] = 0;
    storage::DualPagePointer* pointer = pointers[b];
    if (pointer == nullptr) {
      continue;
    }
    // followed_snapshots is both input and output.
    // as input, it should indicate whether the parent is snapshot or not
    ASSERT_ND(parents[b]->get_header().snapshot_ == followed_snapshots[b]);
    if (pointer->snapshot_pointer_ != 0 && pointer->volatile_pointer_.is_null()) {
      has_some_snapshot = true;
      snapshot_page_ids[b] = pointer->snapshot_pointer_;
    }
  }

  // follow them in a batch. output to tmp_out.
  if (has_some_snapshot) {
    CHECK_ERROR_CODE(find_or_read_snapshot_pages_batch(batch_size, snapshot_page_ids, tmp_out));
  }

  // handle cases we have to volatile pages. also we might have to create a new page.
  for (uint16_t b = 0; b < batch_size; ++b) {
    storage::DualPagePointer* pointer = pointers[b];
    if (has_some_snapshot) {
      if (pointer == nullptr) {
        out[b] = nullptr;
        continue;
      } else if (tmp_out[b]) {
        // if we follow a snapshot pointer _from volatile page_, remember pointer set
        if (needs_ptr_set && !followed_snapshots[b]) {
          current_xct_.add_to_pointer_set(&pointer->volatile_pointer_, pointer->volatile_pointer_);
        }
        followed_snapshots[b] = true;
        out[b] = tmp_out[b];
        continue;
      }
      ASSERT_ND(tmp_out[b] == nullptr);
    }

    // we didn't follow snapshot page. we must follow volatile page, or null.
    followed_snapshots[b] = false;
    ASSERT_ND(!parents[b]->get_header().snapshot_);
    if (pointer->snapshot_pointer_ == 0) {
      if (pointer->volatile_pointer_.is_null()) {
        // both null, so the page is not created yet.
        if (tolerate_null_pointer) {
          out[b] = nullptr;
        } else {
          memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
          if (UNLIKELY(offset == 0)) {
            return kErrorCodeMemoryNoFreePages;
          }
          storage::Page* new_page = local_volatile_page_resolver_.resolve_offset_newpage(offset);
          storage::VolatilePagePointer new_page_id;
          new_page_id.components.numa_node = numa_node_;
          new_page_id.components.offset = offset;
          storage::VolatilePageInitArguments args = {
            holder_,
            new_page_id,
            new_page,
            parents[b],
            index_in_parents[b]
          };
          page_initializer(args);
          storage::assert_valid_volatile_page(new_page, offset);
          ASSERT_ND(new_page->get_header().snapshot_ == false);

          out[b] = place_a_new_volatile_page(offset, pointer);
        }
      } else {
        out[b] = global_volatile_page_resolver_.resolve_offset(pointer->volatile_pointer_);
      }
    } else {
      ASSERT_ND(!pointer->volatile_pointer_.is_null());
      out[b] = global_volatile_page_resolver_.resolve_offset(pointer->volatile_pointer_);
    }
  }
  return kErrorCodeOk;
}

ErrorCode ThreadPimpl::follow_page_pointers_for_write_batch(
  uint16_t batch_size,
  storage::VolatilePageInit page_initializer,
  storage::DualPagePointer** pointers,
  storage::Page** parents,
  const uint16_t* index_in_parents,
  storage::Page** out) {
  // REMINDER: Remember that it might be parents == out. It's not an issue in this function, tho.
  // this method is not quite batched as it doesn't need to be.
  // still, less branches because we can assume all of them need a writable volatile page.
  for (uint16_t b = 0; b < batch_size; ++b) {
    storage::DualPagePointer* pointer = pointers[b];
    if (pointer == nullptr) {
      out[b] = nullptr;
      continue;
    }
    ASSERT_ND(!parents[b]->get_header().snapshot_);
    storage::Page** page = out + b;
    storage::VolatilePagePointer volatile_pointer = pointer->volatile_pointer_;
    if (!volatile_pointer.is_null()) {
      *page = global_volatile_page_resolver_.resolve_offset(volatile_pointer);
    } else if (pointer->snapshot_pointer_ == 0) {
      // we need a volatile page. so construct it from snapshot
      CHECK_ERROR_CODE(install_a_volatile_page(pointer, page));
    } else {
      ASSERT_ND(page_initializer);
      // we must not install a new volatile page in snapshot page. We must not hit this case.
      memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
      if (UNLIKELY(offset == 0)) {
        return kErrorCodeMemoryNoFreePages;
      }
      storage::Page* new_page = local_volatile_page_resolver_.resolve_offset_newpage(offset);
      storage::VolatilePagePointer new_page_id;
      new_page_id.components.numa_node = numa_node_;
      new_page_id.components.offset = offset;
      storage::VolatilePageInitArguments args = {
        holder_,
        new_page_id,
        new_page,
        parents[b],
        index_in_parents[b]
      };
      page_initializer(args);
      storage::assert_valid_volatile_page(new_page, offset);
      ASSERT_ND(new_page->get_header().snapshot_ == false);

      *page = place_a_new_volatile_page(offset, pointer);
    }
    ASSERT_ND(out[b] != nullptr);
  }
  return kErrorCodeOk;
}

ErrorCode ThreadPimpl::find_or_read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page** out) {
  if (snapshot_cache_hashtable_) {
    ASSERT_ND(engine_->get_options().cache_.snapshot_cache_enabled_);
    memory::PagePoolOffset offset = snapshot_cache_hashtable_->find(page_id);
    // the "find" is very efficient and wait-free, but instead it might have false positive/nagative
    // in which case we should just install a new page. No worry about duplicate thanks to the
    // immutability of snapshot pages. it just wastes a bit of CPU and memory.
    if (offset == 0 || snapshot_page_pool_->get_base()[offset].get_header().page_id_ != page_id) {
      if (offset != 0) {
        DVLOG(0) << "Interesting, this race is rare, but possible. offset=" << offset;
      }
      CHECK_ERROR_CODE(on_snapshot_cache_miss(page_id, &offset));
      ASSERT_ND(offset != 0);
      CHECK_ERROR_CODE(snapshot_cache_hashtable_->install(page_id, offset));
      ++control_block_->stat_snapshot_cache_misses_;
    } else {
      ++control_block_->stat_snapshot_cache_hits_;
    }
    ASSERT_ND(offset != 0);
    *out = snapshot_page_pool_->get_base() + offset;
  } else {
    ASSERT_ND(!engine_->get_options().cache_.snapshot_cache_enabled_);
    // Snapshot is disabled. So far this happens only in performance experiments.
    // We use local work memory in this case.
    CHECK_ERROR_CODE(current_xct_.acquire_local_work_memory(
      storage::kPageSize,
      reinterpret_cast<void**>(out),
      storage::kPageSize));
    return read_a_snapshot_page(page_id, *out);
  }
  return kErrorCodeOk;
}

static_assert(
  static_cast<int>(Thread::kMaxFindPagesBatch)
    <= static_cast<int>(cache::CacheHashtable::kMaxFindBatchSize),
  "Booo");

ErrorCode ThreadPimpl::find_or_read_snapshot_pages_batch(
  uint16_t batch_size,
  const storage::SnapshotPagePointer* page_ids,
  storage::Page** out) {
  ASSERT_ND(batch_size <= Thread::kMaxFindPagesBatch);
  if (batch_size == 0) {
    return kErrorCodeOk;
  } else if (UNLIKELY(batch_size > Thread::kMaxFindPagesBatch)) {
    return kErrorCodeInvalidParameter;
  }

  if (snapshot_cache_hashtable_) {
    ASSERT_ND(engine_->get_options().cache_.snapshot_cache_enabled_);
    memory::PagePoolOffset offsets[Thread::kMaxFindPagesBatch];
    CHECK_ERROR_CODE(snapshot_cache_hashtable_->find_batch(batch_size, page_ids, offsets));
    for (uint16_t b = 0; b < batch_size; ++b) {
      memory::PagePoolOffset offset = offsets[b];
      storage::SnapshotPagePointer page_id = page_ids[b];
      if (page_id == 0) {
        out[b] = nullptr;
        continue;
      } else if (b > 0 && page_ids[b - 1] == page_id) {
        ASSERT_ND(offsets[b - 1] == offset);
        out[b] = out[b - 1];
        continue;
      }
      if (offset == 0 || snapshot_page_pool_->get_base()[offset].get_header().page_id_ != page_id) {
        if (offset != 0) {
          DVLOG(0) << "Interesting, this race is rare, but possible. offset=" << offset;
        }
        CHECK_ERROR_CODE(on_snapshot_cache_miss(page_id, &offset));
        ASSERT_ND(offset != 0);
        CHECK_ERROR_CODE(snapshot_cache_hashtable_->install(page_id, offset));
        ++control_block_->stat_snapshot_cache_misses_;
      } else {
        ++control_block_->stat_snapshot_cache_hits_;
      }
      ASSERT_ND(offset != 0);
      out[b] = snapshot_page_pool_->get_base() + offset;
    }
  } else {
    ASSERT_ND(!engine_->get_options().cache_.snapshot_cache_enabled_);
    for (uint16_t b = 0; b < batch_size; ++b) {
      CHECK_ERROR_CODE(current_xct_.acquire_local_work_memory(
        storage::kPageSize,
        reinterpret_cast<void**>(out + b),
        storage::kPageSize));
      CHECK_ERROR_CODE(read_a_snapshot_page(page_ids[b], out[b]));
    }
  }
  return kErrorCodeOk;
}


ErrorCode ThreadPimpl::on_snapshot_cache_miss(
  storage::SnapshotPagePointer page_id,
  memory::PagePoolOffset* pool_offset) {
  // grab a buffer page to read into.
  memory::PagePoolOffset offset = core_memory_->grab_free_snapshot_page();
  if (offset == 0) {
    // TASK(Hideaki) First, we have to make sure this doesn't happen often (cleaner's work).
    // Second, when this happens, we have to do eviction now, but probably after aborting the xct.
    LOG(ERROR) << "Could not grab free snapshot page while cache miss. thread=" << *holder_
      << ", page_id=" << assorted::Hex(page_id);
    return kErrorCodeCacheNoFreePages;
  }

  storage::Page* new_page = snapshot_page_pool_->get_base() + offset;
  ErrorCode read_result = read_a_snapshot_page(page_id, new_page);
  if (read_result != kErrorCodeOk) {
    LOG(ERROR) << "Failed to read a snapshot page. thread=" << *holder_
      << ", page_id=" << assorted::Hex(page_id);
    core_memory_->release_free_snapshot_page(offset);
    return read_result;
  }

  *pool_offset = offset;
  return kErrorCodeOk;
}

////////////////////////////////////////////////////////////////////////////////
///
///      Retired page handling methods
///
////////////////////////////////////////////////////////////////////////////////
void ThreadPimpl::collect_retired_volatile_page(storage::VolatilePagePointer ptr) {
  ASSERT_ND(is_volatile_page_retired(ptr));
  uint16_t node = ptr.components.numa_node;
  Epoch current_epoch = engine_->get_xct_manager()->get_current_global_epoch_weak();
  Epoch safe_epoch = current_epoch.one_more().one_more();
  memory::PagePoolOffsetAndEpochChunk* chunk = core_memory_->get_retired_volatile_pool_chunk(node);
  if (chunk->full()) {
    flush_retired_volatile_page(node, current_epoch, chunk);
  }
  chunk->push_back(ptr.components.offset, safe_epoch);
}

void ThreadPimpl::flush_retired_volatile_page(
  uint16_t node,
  Epoch current_epoch,
  memory::PagePoolOffsetAndEpochChunk* chunk) {
  if (chunk->size() == 0) {
    return;
  }
  uint32_t safe_count = chunk->get_safe_offset_count(current_epoch);
  while (safe_count < chunk->size() / 10U) {
    LOG(WARNING) << "Thread-" << id_ << " can return only "
      << safe_count << " out of " << chunk->size()
      << " retired pages to node-" << node  << " in epoch=" << current_epoch
      << ". This means the thread received so many retired pages in a short time period."
      << " Will adavance an epoch to safely return the retired pages."
      << " This should be a rare event.";
    engine_->get_xct_manager()->advance_current_global_epoch();
    current_epoch = engine_->get_xct_manager()->get_current_global_epoch();
    LOG(INFO) << "okay, advanced epoch. now we should be able to return more pages";
    safe_count = chunk->get_safe_offset_count(current_epoch);
  }

  VLOG(0) << "Thread-" << id_ << " batch-returning retired volatile pages to node-" << node
    << " safe_count/count=" << safe_count << "/" << chunk->size() << ". epoch=" << current_epoch;
  memory::PagePool* volatile_pool
    = engine_->get_memory_manager()->get_node_memory(node)->get_volatile_pool();
  volatile_pool->release(safe_count, chunk);
  ASSERT_ND(!chunk->full());
}

bool ThreadPimpl::is_volatile_page_retired(storage::VolatilePagePointer ptr) {
  storage::Page* page = global_volatile_page_resolver_.resolve_offset(ptr);
  return page->get_header().page_version_.is_retired();
}

////////////////////////////////////////////////////////////////////////////////
///
///      MCS Locking methods
///
////////////////////////////////////////////////////////////////////////////////
xct::McsRwBlock* ThreadPimpl::get_mcs_rw_block(thread::ThreadId id, xct::McsBlockIndex index) {
  ASSERT_ND(index > 0);
  ASSERT_ND(index < 0xFFFFU);
  ThreadRef thread = pool_pimpl_->get_thread_ref(id);
  return thread.get_mcs_rw_blocks() + index;
}
xct::McsRwBlock* ThreadPimpl::get_mcs_rw_block(uint32_t tail_int) {
  xct::McsRwLock lock;
  lock.tail_ = tail_int;
  ThreadRef thread = pool_pimpl_->get_thread_ref(lock.get_tail_waiter());
  return thread.get_mcs_rw_blocks() + lock.get_tail_waiter_block();
}

xct::McsRwBlock* Thread::get_mcs_rw_blocks() {
  return pimpl_->mcs_rw_blocks_;
}

// Put Thread methods here to allow inlining.
xct::McsBlockIndex Thread::mcs_acquire_lock(xct::McsLock* mcs_lock) {
  return pimpl_->mcs_acquire_lock(mcs_lock);
}
xct::McsBlockIndex Thread::mcs_acquire_lock_batch(xct::McsLock** mcs_locks, uint16_t batch_size) {
  // lock in address order. so, no deadlock possible
  // we have to lock them whether the record is deleted or not. all physical records.
  xct::McsBlockIndex head_lock_index = 0;
  for (uint16_t i = 0; i < batch_size; ++i) {
    xct::McsBlockIndex block = pimpl_->mcs_acquire_lock(mcs_locks[i]);
    ASSERT_ND(block > 0);
    if (i == 0) {
      head_lock_index = block;
    } else {
      ASSERT_ND(head_lock_index + i == block);
    }
  }
  return head_lock_index;
}
void Thread::mcs_release_lock_batch(
  xct::McsLock** mcs_locks,
  xct::McsBlockIndex head_block,
  uint16_t batch_size) {
  for (uint16_t i = 0; i < batch_size; ++i) {
    pimpl_->mcs_release_lock(mcs_locks[i], head_block + i);
  }
}

xct::McsBlockIndex Thread::mcs_initial_lock(xct::McsLock* mcs_lock) {
  return pimpl_->mcs_initial_lock(mcs_lock);
}
void Thread::mcs_release_lock(xct::McsLock* mcs_lock, xct::McsBlockIndex block_index) {
  pimpl_->mcs_release_lock(mcs_lock, block_index);
}

xct::McsBlockIndex Thread::mcs_acquire_reader_lock(xct::McsRwLock* mcs_rw_lock) {
  return pimpl_->mcs_acquire_reader_lock(mcs_rw_lock);
}

void Thread::mcs_release_reader_lock(xct::McsRwLock* mcs_rw_lock, xct::McsBlockIndex block_index) {
  pimpl_->mcs_release_reader_lock(mcs_rw_lock, block_index);
}

xct::McsBlockIndex Thread::mcs_acquire_writer_lock(xct::McsRwLock* mcs_rw_lock) {
  return pimpl_->mcs_acquire_writer_lock(mcs_rw_lock);
}

void Thread::mcs_release_writer_lock(xct::McsRwLock* mcs_rw_lock, xct::McsBlockIndex block_index) {
  pimpl_->mcs_release_writer_lock(mcs_rw_lock, block_index);
}

bool Thread::mcs_try_acquire_reader_lock(
  xct::McsRwLock* mcs_rw_lock, xct::McsBlockIndex* out_block_index, int tries) {
  return pimpl_->mcs_try_acquire_reader_lock(mcs_rw_lock, out_block_index, tries);
}

bool Thread::mcs_retry_acquire_reader_lock(
  xct::McsRwLock* mcs_rw_lock, xct::McsBlockIndex block_index, bool wait_for_result) {
  return pimpl_->mcs_retry_acquire_reader_lock(mcs_rw_lock, block_index, wait_for_result);
}

bool Thread::mcs_try_acquire_writer_lock(
  xct::McsRwLock* mcs_rw_lock, xct::McsBlockIndex* out_block_index, int tries) {
  return pimpl_->mcs_try_acquire_writer_lock(mcs_rw_lock, out_block_index, tries);
}

bool Thread::mcs_retry_acquire_writer_lock(
  xct::McsRwLock* mcs_rw_lock, xct::McsBlockIndex block_index, bool wait_for_result) {
  return pimpl_->mcs_retry_acquire_writer_lock(mcs_rw_lock, block_index, wait_for_result);
}

xct::McsBlockIndex Thread::mcs_try_upgrade_reader_lock(
  xct::McsRwLock* mcs_rw_lock,
  xct::McsBlockIndex block_index) {
  return pimpl_->mcs_try_upgrade_reader_lock(mcs_rw_lock, block_index);
}

void Thread::mcs_ownerless_initial_lock(xct::McsLock* mcs_lock) {
  ThreadPimpl::mcs_ownerless_initial_lock(mcs_lock);
}

void Thread::mcs_ownerless_acquire_lock(xct::McsLock* mcs_lock) {
  ThreadPimpl::mcs_ownerless_acquire_lock(mcs_lock);
}

void Thread::mcs_ownerless_release_lock(xct::McsLock* mcs_lock) {
  ThreadPimpl::mcs_ownerless_release_lock(mcs_lock);
}

inline void assert_mcs_aligned(const void* address) {
  ASSERT_ND(address);
  ASSERT_ND(reinterpret_cast<uintptr_t>(address) % 4 == 0);
}

void ThreadPimpl::mcs_toolong_wait(
  xct::McsRwLock* mcs_lock,
  ThreadId predecessor_id,
  xct::McsBlockIndex my_block,
  xct::McsBlockIndex pred_block) {
  uint32_t count = current_xct_.get_write_set_size();
  xct::WriteXctAccess* write_sets = current_xct_.get_write_set();
  uint32_t index = count + 1;
  for (uint32_t i = 0; i < count; ++i) {
    if (i > 0 && write_sets[i - 1].owner_id_address_ > write_sets[i].owner_id_address_) {
      LOG(ERROR) << "mmm? unsorted?? this might be not within precommit, which is possible";
    }
    if (write_sets[i].owner_id_address_->get_key_lock() == mcs_lock) {
      index = i;
    }
  }
  LOG(ERROR) << "I'm waiting here for long time. me=" << id_ << "(block=" << my_block << "), pred="
    << predecessor_id << "(block=" << pred_block << "), lock=" << *mcs_lock
    << ", lock_addr=" << mcs_lock
    << ", write-set " << index << "/" << count;
}

xct::McsBlockIndex ThreadPimpl::mcs_acquire_lock(xct::McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient mfences (note, xchg implies lock prefix. not a compiler's bug!).
  ASSERT_ND(!control_block_->mcs_waiting_);
  assert_mcs_aligned(mcs_lock);
  // so far we allow only 2^16 MCS blocks per transaction. we might increase later.
  ASSERT_ND(current_xct_.get_mcs_block_current() < 0xFFFFU);
  xct::McsBlockIndex block_index = current_xct_.increment_mcs_block_current();
  ASSERT_ND(block_index > 0);
  ASSERT_ND(block_index <= 0xFFFFU);
  xct::McsBlock* my_block = mcs_blocks_ + block_index;
  my_block->clear_successor_release();
  control_block_->mcs_waiting_.store(true, std::memory_order_release);
  uint32_t desired = xct::McsLock::to_int(id_, block_index);
  uint32_t group_tail = desired;
  uint32_t* address = &(mcs_lock->data_);
  assert_mcs_aligned(address);

  uint32_t pred_int = 0;
  while (true) {
    // if it's obviously locked by a guest, we should wait until it's released.
    // so far this is busy-wait, we can do sth. to prevent priority inversion later.
    if (UNLIKELY(*address == xct::kMcsGuestId)) {
      spin_until([address]{
        return assorted::atomic_load_acquire<uint32_t>(address) == xct::kMcsGuestId;
      });
    }

    // atomic op should imply full barrier, but make sure announcing the initialized new block.
    ASSERT_ND(group_tail != xct::kMcsGuestId);
    ASSERT_ND(group_tail != 0);
    ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != group_tail);
    pred_int = assorted::raw_atomic_exchange<uint32_t>(address, group_tail);
    ASSERT_ND(pred_int != group_tail);
    ASSERT_ND(pred_int != desired);

    if (pred_int == 0) {
      // this means it was not locked.
      ASSERT_ND(mcs_lock->is_locked());
      DVLOG(2) << "Okay, got a lock uncontended. me=" << id_;
      control_block_->mcs_waiting_.store(false, std::memory_order_release);
      ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
      return block_index;
    } else if (UNLIKELY(pred_int == xct::kMcsGuestId)) {
      // ouch, I don't want to keep the guest ID! return it back.
      // This also determines the group_tail of this queue
      group_tail = assorted::raw_atomic_exchange<uint32_t>(address, xct::kMcsGuestId);
      ASSERT_ND(group_tail != 0 && group_tail != xct::kMcsGuestId);
      continue;
    } else {
      break;
    }
  }

  ASSERT_ND(pred_int != 0 && pred_int != xct::kMcsGuestId);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != xct::kMcsGuestId);
  xct::McsLock old;
  old.data_ = pred_int;
  ASSERT_ND(mcs_lock->is_locked());
  ThreadId predecessor_id = old.get_tail_waiter();
  ASSERT_ND(predecessor_id != id_);
  xct::McsBlockIndex predecessor_block = old.get_tail_waiter_block();
  DVLOG(0) << "mm, contended, we have to wait.. me=" << id_ << " pred=" << predecessor_id;
  ASSERT_ND(decompose_numa_node(predecessor_id) < engine_->get_options().thread_.group_count_);
  ASSERT_ND(decompose_numa_local_ordinal(predecessor_id) <
    engine_->get_options().thread_.thread_count_per_group_);

  ThreadRef predecessor = pool_pimpl_->get_thread_ref(predecessor_id);
  ASSERT_ND(control_block_->mcs_waiting_);
  ASSERT_ND(predecessor.get_control_block()->mcs_block_current_ >= predecessor_block);
  xct::McsBlock* pred_block = predecessor.get_mcs_blocks() + predecessor_block;
  ASSERT_ND(!pred_block->has_successor());
#ifndef NDEBUG
  if (UNLIKELY(predecessor.get_control_block()->my_thread_id_ != predecessor_id)) {
    LOG(FATAL) << "wtf. predecessor_id=" << predecessor_id
      << ", but " << predecessor.get_control_block()->my_thread_id_;
  }
#endif  // NDEBUG

  pred_block->set_successor_release(id_, block_index);

  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != xct::kMcsGuestId);
  spin_until([this]{ return this->control_block_->mcs_waiting_.load(std::memory_order_acquire); });
  DVLOG(1) << "Okay, now I hold the lock. me=" << id_ << ", ex-pred=" << predecessor_id;
  ASSERT_ND(!control_block_->mcs_waiting_);
  ASSERT_ND(mcs_lock->is_locked());
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != xct::kMcsGuestId);
  return block_index;
}

void ThreadPimpl::mcs_ownerless_acquire_lock(xct::McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient mfences (note, xchg implies lock prefix. not a compiler's bug!).
  assert_mcs_aligned(mcs_lock);
  uint32_t* address = &(mcs_lock->data_);
  assert_mcs_aligned(address);
  spin_until([mcs_lock, address]{
    uint32_t old_int = xct::McsLock::to_int(0, 0);
    return !assorted::raw_atomic_compare_exchange_weak<uint32_t>(
      address,
      &old_int,
      xct::kMcsGuestId);
  });
  DVLOG(1) << "Okay, now I hold the lock. me=guest";
  ASSERT_ND(mcs_lock->is_locked());
}
xct::McsBlockIndex ThreadPimpl::mcs_initial_lock(xct::McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with release barrier.
  // This method itself doesn't need barriers, but then we need to later take a seq_cst barrier
  // in an appropriate place. That's hard to debug, so just take release barriers here.
  // Also, everything should be inlined.
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(!control_block_->mcs_waiting_);
  ASSERT_ND(!mcs_lock->is_locked());
  // so far we allow only 2^16 MCS blocks per transaction. we might increase later.
  ASSERT_ND(current_xct_.get_mcs_block_current() < 0xFFFFU);
  xct::McsBlockIndex block_index = current_xct_.increment_mcs_block_current();
  ASSERT_ND(block_index > 0 && block_index <= 0xFFFFU);
  xct::McsBlock* my_block = mcs_blocks_ + block_index;
  my_block->clear_successor_release();
  mcs_lock->reset_release(id_, block_index);
  return block_index;
}

void ThreadPimpl::mcs_ownerless_initial_lock(xct::McsLock* mcs_lock) {
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(!mcs_lock->is_locked());
  mcs_lock->reset_guest_id_release();
}

void ThreadPimpl::mcs_release_lock(xct::McsLock* mcs_lock, xct::McsBlockIndex block_index) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient lock/mfences.
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(!control_block_->mcs_waiting_);
  ASSERT_ND(mcs_lock->is_locked());
  ASSERT_ND(block_index > 0);
  ASSERT_ND(current_xct_.get_mcs_block_current() >= block_index);
  const uint32_t myself = xct::McsLock::to_int(id_, block_index);
  uint32_t* address = &(mcs_lock->data_);
  xct::McsBlock* block = mcs_blocks_ + block_index;
  if (!block->has_successor()) {
    // okay, successor "seems" nullptr (not contended), but we have to make it sure with atomic CAS
    uint32_t expected = myself;
    assert_mcs_aligned(address);
    bool swapped = assorted::raw_atomic_compare_exchange_strong<uint32_t>(address, &expected, 0);
    if (swapped) {
      // we have just unset the locked flag, but someone else might have just acquired it,
      // so we can't put assertion here.
      ASSERT_ND(id_ == 0 || mcs_lock->get_tail_waiter() != id_);
      ASSERT_ND(expected == myself);
      ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);
      DVLOG(2) << "Okay, release a lock uncontended. me=" << id_;
      return;
    }
    ASSERT_ND(expected != 0);
    ASSERT_ND(expected != xct::kMcsGuestId);
    DVLOG(0) << "Interesting contention on MCS release. I thought it's null, but someone has just "
      " jumped in. me=" << id_ << ", mcs_lock=" << *mcs_lock;
    // wait for someone else to set the successor
    ASSERT_ND(mcs_lock->is_locked());
    if (UNLIKELY(!block->has_successor())) {
      spin_until([block]{ return !block->has_successor_atomic(); });
    }
  }
  ThreadId successor_id = block->get_successor_thread_id();
  DVLOG(1) << "Okay, I have a successor. me=" << id_ << ", succ=" << successor_id;
  ASSERT_ND(successor_id != id_);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);

  ThreadRef successor = pool_pimpl_->get_thread_ref(successor_id);
  ThreadControlBlock* successor_cb = successor.get_control_block();
  ASSERT_ND(successor_cb->mcs_block_current_ >= block->get_successor_block());
  ASSERT_ND(successor_cb->mcs_waiting_);
  ASSERT_ND(mcs_lock->is_locked());
#ifndef NDEBUG
  if (UNLIKELY(successor_cb->my_thread_id_ != successor_id)) {
    LOG(FATAL) << "wtf. successor_id=" << successor_id << ", but " << successor_cb->my_thread_id_;
  } else if (UNLIKELY(!successor_cb->mcs_waiting_.load())) {
    LOG(FATAL) << "wtf";
  }
#endif  // NDEBUG

  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);
  successor_cb->mcs_waiting_.store(false, std::memory_order_release);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);
}
void ThreadPimpl::mcs_ownerless_release_lock(xct::McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient mfences (note, xchg implies lock prefix. not a compiler's bug!).
  assert_mcs_aligned(mcs_lock);
  uint32_t* address = &(mcs_lock->data_);
  assert_mcs_aligned(address);
  ASSERT_ND(mcs_lock->is_locked());
  spin_until([address]{
    uint32_t old_int = xct::kMcsGuestId;
    return !assorted::raw_atomic_compare_exchange_weak<uint32_t>(address, &old_int, 0);
  });
  DVLOG(1) << "Okay, guest released the lock.";
}


xct::McsBlockIndex ThreadPimpl::mcs_acquire_reader_lock(xct::McsRwLock* mcs_rw_lock) {
  return 0;
  /*
  ASSERT_ND(current_xct_.get_mcs_block_current() < 0xFFFFU);
  xct::McsBlockIndex block_index = current_xct_.increment_mcs_block_current();
  ASSERT_ND(block_index > 0);
  // TODO(tzwang): make this a static_size_check...
  ASSERT_ND(sizeof(xct::McsRwBlock) == sizeof(xct::McsBlock));
  xct::McsRwBlock* my_block = (xct::McsRwBlock *)mcs_blocks_ + block_index;

  // So I'm a reader
  my_block->init_reader();
  ASSERT_ND(my_block->is_blocked() && my_block->is_reader());
  ASSERT_ND(!my_block->has_successor());
  ASSERT_ND(my_block->successor_block_index_ == 0);

  // Now ready to XCHG
  uint32_t tail_desired = xct::McsRwLock::to_tail_int(id_, block_index);
  uint32_t* tail_address = &(mcs_rw_lock->tail_);
  uint32_t pred_tail_int = assorted::raw_atomic_exchange<uint32_t>(tail_address, tail_desired);

  if (pred_tail_int == 0) {
    mcs_rw_lock->increment_readers_count();
    my_block->unblock();  // reader successors will know they don't need to wait
  } else {
    // See if the predecessor is a reader; if so, if it already acquired the lock.
    xct::McsRwLock old;
    old.tail_ = pred_tail_int;
    xct::McsBlockIndex pred_block_index = old.get_tail_waiter_block();
    ThreadId pred_id = old.get_tail_waiter();
    ThreadRef pred = pool_pimpl_->get_thread_ref(pred_id);
    xct::McsRwBlock* pred_block = (xct::McsRwBlock *)pred.get_mcs_blocks() + pred_block_index;
    uint16_t* pred_state_address = &pred_block->self_.data_;
    uint16_t pred_state_expected = pred_block->make_blocked_with_no_successor_state();
    uint16_t pred_state_desired = pred_block->make_blocked_with_reader_successor_state();
    if (!pred_block->is_reader() || assorted::raw_atomic_compare_exchange_strong<uint16_t>(
      pred_state_address,
      &pred_state_expected,
      pred_state_desired)) {
      // Predecessor is a writer or a waiting reader. The successor class field and the
      // blocked state in pred_block are separated, so we can blindly set_successor().
      pred_block->set_successor_next_only(id_, block_index);
      spin_until([my_block]{ return my_block->is_blocked(); });
    } else {
      // Join the active, reader predecessor
      ASSERT_ND(!pred_block->is_blocked());
      mcs_rw_lock->increment_readers_count();
      pred_block->set_successor_next_only(id_, block_index);
      my_block->unblock();
    }
  }

  if (my_block->has_reader_successor()) {
    spin_until([my_block]{ return !my_block->successor_is_ready(); });
    // Unblock the reader successor
    thread::ThreadRef successor = pool_pimpl_->get_thread_ref(my_block->successor_thread_id_);
    xct::McsRwBlock* successor_block =
      (xct::McsRwBlock *)successor.get_mcs_blocks() + my_block->successor_block_index_;
    mcs_rw_lock->increment_readers_count();
    successor_block->unblock();
  }
  return block_index;
  */
}

/*
void ThreadPimpl::mcs_release_reader_lock(
  xct::McsRwLock* mcs_rw_lock,
  xct::McsBlockIndex block_index) {
  ASSERT_ND(block_index > 0);
  ASSERT_ND(current_xct_.get_mcs_block_current() >= block_index);
  xct::McsRwBlock* my_block = (xct::McsRwBlock *)mcs_blocks_ + block_index;
//  ASSERT_ND(my_block->is_normal());
  // Make sure there is really no successor or wait for it
  uint32_t* tail_address = &mcs_rw_lock->tail_;
  uint32_t expected = xct::McsRwLock::to_tail_int(id_, block_index);
//  my_block->toggle_allocated();
//  ASSERT_ND(!my_block->is_allocated());
//  ASSERT_ND(my_block->is_normal());
  //LOG(INFO) << id_ << " releasing reader lock " << block_index;
  if (my_block->successor_is_ready() ||
    !assorted::raw_atomic_compare_exchange_strong<uint32_t>(tail_address, &expected, 0)) {
    // Have to wait for the successor to install itself after me
    // Don't check for curr_block->has_successor()! It only tells whether the state bit
    // is set, not whether successor_thread_id_ and successor_block_index_ are set.
    // But remember to skip trying readers who failed.
    spin_until([my_block]{ return !(my_block->successor_is_ready()); });
    if (my_block->has_writer_successor()) {
      assorted::raw_atomic_exchange<ThreadId>(
        &mcs_rw_lock->next_writer_,
        my_block->successor_thread_id_);
    }
  }

  if (mcs_rw_lock->decrement_readers_count() == 1) {
    // I'm the last active reader
    ThreadId next_writer = assorted::atomic_load_acquire<ThreadId>(&mcs_rw_lock->next_writer_);
    if (next_writer != xct::McsRwLock::kNextWriterNone &&
        mcs_rw_lock->nreaders() == 0 &&
        assorted::raw_atomic_compare_exchange_strong<ThreadId>(
          &mcs_rw_lock->next_writer_,
          &next_writer,
          xct::McsRwLock::kNextWriterNone)) {
      // I have a waiting writer, wake it up
      ThreadRef writer = pool_pimpl_->get_thread_ref(next_writer);
      // Assuming a thread can wait for one and only one MCS lock at any instant
      // before starting to acquire the next.
      xct::McsRwBlock *writer_block =
        (xct::McsRwBlock *)writer.get_mcs_blocks() + writer.get_control_block()->mcs_block_current_;
      ASSERT_ND(writer_block->is_blocked());
//      ASSERT_ND(writer_block->is_allocated());
      ASSERT_ND(!writer_block->is_reader());
      writer_block->unblock();
    }
  }
//  ASSERT_ND(!my_block->is_allocated());
}
  */

xct::McsBlockIndex ThreadPimpl::mcs_acquire_writer_lock(xct::McsRwLock* mcs_rw_lock) {
  return 0;
  /*
  ASSERT_ND(current_xct_.get_mcs_block_current() < 0xFFFFU);
  xct::McsBlockIndex block_index = current_xct_.increment_mcs_block_current();
  ASSERT_ND(block_index > 0);
  // TODO(tzwang): make this a static_size_check...
  ASSERT_ND(sizeof(xct::McsRwBlock) == sizeof(xct::McsBlock));
  xct::McsRwBlock* my_block = (xct::McsRwBlock *)mcs_blocks_ + block_index;

  my_block->init_writer();
  ASSERT_ND(my_block->is_blocked() && !my_block->is_reader());
  ASSERT_ND(!my_block->has_successor());
  ASSERT_ND(my_block->successor_block_index_ == 0);

  // Now ready to XCHG
  uint32_t tail_desired = xct::McsRwLock::to_tail_int(id_, block_index);
  uint32_t* tail_address = &(mcs_rw_lock->tail_);
  uint32_t pred_tail_int = assorted::raw_atomic_exchange<uint32_t>(tail_address, tail_desired);
  ASSERT_ND(pred_tail_int != tail_desired);
  ThreadId old_next_writer = 0xFFFFU;
  if (pred_tail_int == 0) {
    assorted::raw_atomic_exchange<ThreadId>(&mcs_rw_lock->next_writer_, id_);
    if (mcs_rw_lock->nreaders() == 0) {
      old_next_writer = assorted::raw_atomic_exchange<ThreadId>(
        &mcs_rw_lock->next_writer_,
        xct::McsRwLock::kNextWriterNone);
      if (old_next_writer == id_) {
        my_block->unblock();
        return block_index;
      }
    }
  } else {
    xct::McsRwLock old;
    old.tail_ = pred_tail_int;
    xct::McsBlockIndex pred_block_index = old.get_tail_waiter_block();
    ThreadId pred_id = old.get_tail_waiter();
    ThreadRef pred = pool_pimpl_->get_thread_ref(pred_id);
    xct::McsRwBlock* pred_block = (xct::McsRwBlock *)pred.get_mcs_blocks() + pred_block_index;
    pred_block->set_successor_class_writer();
    pred_block->set_successor_next_only(id_, block_index);
  }
  spin_until([my_block]{ return my_block->is_blocked(); });
  return block_index;
  */
}

  /*
void ThreadPimpl::mcs_release_writer_lock(
  xct::McsRwLock* mcs_rw_lock,
  xct::McsBlockIndex block_index) {
  ASSERT_ND(block_index > 0);
  ASSERT_ND(current_xct_.get_mcs_block_current() >= block_index);
  xct::McsRwBlock* my_block = (xct::McsRwBlock *)mcs_blocks_ + block_index;
  uint32_t expected = xct::McsRwLock::to_tail_int(id_, block_index);
  uint32_t* tail_address = &mcs_rw_lock->tail_;
  if (my_block->successor_is_ready() ||
    !assorted::raw_atomic_compare_exchange_strong<uint32_t>(tail_address, &expected, 0)) {
    if (UNLIKELY(!my_block->successor_is_ready())) {
      spin_until([my_block]{ return !(my_block->successor_is_ready()); });
    }
    ASSERT_ND(my_block->successor_is_ready());
    auto* successor_block = get_mcs_rw_block(
      my_block->successor_thread_id_,
      my_block->successor_block_index_);
    ASSERT_ND(successor_block->is_blocked());
    if (successor_block->is_reader()) {
      mcs_rw_lock->increment_readers_count();
    }
    successor_block->unblock();
  }
  //LOG(INFO) << id_ << " released writer lock " << block_index;
}
*/

/** MCS try-reader-writer lock helpers */
inline void ThreadPimpl::pass_group_tail_to_successor(
  xct::McsRwBlock* block,
  uint32_t my_tail) {
  ASSERT_ND(block->is_aborting());
  spin_until([block] { return block->get_group_tail_int() == 0; });
  uint32_t group_tail = block->get_group_tail_int();
  ASSERT_ND(group_tail);
  ASSERT_ND(my_tail);
  // Exclude myself
  if (my_tail != group_tail) {
    spin_until([block]{ return !block->successor_is_ready(); });
    auto* sb = get_mcs_rw_block(
      block->successor_thread_id_,
      block->successor_block_index_);
    ASSERT_ND(sb->get_group_tail_int() == 0);
    sb->set_group_tail_int(group_tail);
    ASSERT_ND(sb->is_waiting());
    sb->set_state_aborting();
  }
}

inline bool ThreadPimpl::try_abort_as_group_leader(xct::McsRwBlock* expected_holder) {
  ASSERT_ND(expected_holder->successor_thread_id_ == 0);
  ASSERT_ND(expected_holder->successor_block_index_ == 0);
  ASSERT_ND(!expected_holder->is_waiting());

  if (expected_holder->is_writer() && expected_holder->is_releasing()) {
    return true;
  }
  // Prepare to give up
  uint16_t expected = expected_holder->make_granted_with_no_successor_state();
  uint16_t desired = expected_holder->make_granted_with_aborting_successor_state();
  uint16_t *address = &expected_holder->self_.data_;
  if (!assorted::raw_atomic_compare_exchange_weak<uint16_t>(address, &expected, desired)) {
    // Two cases the above CAS might fail:
    // 1. expected_holder changed to releasing state;
    // 2. expected_holder already has_aborting_successor() (ie some previous requestor succeeded
    // the above CAS and gave up).
    // 3. expected_holder is actually not a holder... it is aborting. We should wait for
    // order in this case.
    // Note: if the caller is try_acquire_reader_lock, then expected_holder must be a writer.
    // (if expected_holder is a reader, we'll directly either wait for order or acquire the lock)
    if (expected_holder->is_writer() && expected_holder->is_releasing()) {
      return true;
    }
  }
  // Never set successor if we decide to abort as a leader
  ASSERT_ND(expected_holder->successor_block_index_ == 0);
  return false;
}

const int kLockReaderAcquireRetries = 8;
const int kLockWriterAcquireRetries = 6;
const int kLockReaderUpgradeRetries = 2;
const uint64_t kLockAcquireRetryBackoff = 1 << 2;  // wait for this many cycles before retrying

bool ThreadPimpl::mcs_try_acquire_reader_lock(
  xct::McsRwLock* mcs_rw_lock, xct::McsBlockIndex* out_block_index, int tries) {
retry:
  xct::McsBlockIndex block_index = 0;
  ASSERT_ND(out_block_index);
  xct::McsRwBlock* my_block = NULL;
  if (*out_block_index) {
    block_index = *out_block_index;
    my_block = mcs_rw_blocks_ + block_index;
    ASSERT_ND(!my_block->is_waiting());
    ASSERT_ND(!my_block->is_aborting());
  } else {
    block_index = *out_block_index = current_xct_.increment_mcs_block_current();
    my_block = mcs_rw_blocks_ + block_index;
  }
  ASSERT_ND(block_index > 0);
  ASSERT_ND(block_index <= 0xFFFFU);
  my_block->init_reader();
  ASSERT_ND(my_block->is_waiting());
  ASSERT_ND(my_block->is_reader());
  ASSERT_ND(my_block->successor_block_index_ == 0);

  uint32_t my_tail_int = xct::McsRwLock::to_tail_int(id_, block_index);
  my_block->pred_tail_int_ = mcs_rw_lock->install_tail(my_tail_int);
  if (my_block->pred_tail_int_ == 0) {
    mcs_rw_lock->increment_readers_count();
    my_block->set_state_granted();
    ASSERT_ND(my_block->is_granted());
  } else {
    ASSERT_ND(my_block->pred_tail_int_ >> 16 != id_);
    auto* pred_block = get_mcs_rw_block(my_block->pred_tail_int_);
    ASSERT_ND(pred_block->successor_thread_id_ == 0);
    ASSERT_ND(pred_block->successor_block_index_ == 0);
    // Doesn't matter pred is a writer or reader so far
    uint16_t state_desired = pred_block->make_waiting_with_reader_successor_state();
    uint16_t state_expected = pred_block->make_waiting_with_no_successor_state();
    uint16_t* state_address = &pred_block->self_.data_;
    bool ret = assorted::raw_atomic_compare_exchange_weak<uint16_t>(
      state_address, &state_expected, state_desired);
    if (ret) {
      // attached to a waiting pred, wait for order
      ASSERT_ND(pred_block->has_reader_successor());
      pred_block->set_successor_next_only(id_, block_index);
    } else {
      // pred's state isn't waiting.
      // if pred is a writer and releasing, we should wait and get the lock;
      // if pred is a writer and granted, we should give up;
      // if pred is a writer and aborting, we should wait for its order to abort;
      // we rely on try_abort_as_group_leader to CAS pred's state from [granted|no successor]
      // to [aborting successor] to differentiate the above three cases.
      //
      // If pred is a reader, it's either granted or aborting. In the former
      // case we should get the lock; in the latter case we wait for its order to abort.
      // Also if pred is a reader, then I, as another trying reader, can't be a group leader.
      ASSERT_ND(!pred_block->is_waiting());
      if (pred_block->is_writer()) {
        bool can_get_lock = try_abort_as_group_leader(pred_block);
        if (can_get_lock) {
          ASSERT_ND(!pred_block->has_reader_successor());
          pred_block->set_successor_next_only(id_, block_index);
        } else {
          // See if pred is really a holder
          if (pred_block->is_aborting()) {  // duang!! it's not a holder!
            pred_block->set_successor_next_only(id_, block_index);
          } else {
            my_block->set_state_aborting();
            auto group_tail = mcs_rw_lock->install_tail(my_block->pred_tail_int_);
            ASSERT_ND(group_tail);
            my_block->set_group_tail_int(group_tail);
            pass_group_tail_to_successor(my_block, my_tail_int);
            my_block->set_state_aborted();
            if (--tries) {
              goto retry;
            }
            return false;  // leader can't rely on retry to give up
          }
        }
      } else {
        ASSERT_ND(pred_block->is_reader());
        ASSERT_ND(!pred_block->is_waiting());
        if (pred_block->is_aborting()) {
          // Wait for order (to abort)
          pred_block->set_successor_next_only(id_, block_index);
        } else {
          // pred must be granted
          ASSERT_ND(!pred_block->has_reader_successor());
          mcs_rw_lock->increment_readers_count();
          pred_block->set_successor_next_only(id_, block_index);
          my_block->set_state_granted();
          ASSERT_ND(my_block->is_granted());
        }
      }
    }
  }

  return true;
}

bool ThreadPimpl::mcs_retry_acquire_reader_lock(
  xct::McsRwLock* lock, xct::McsBlockIndex block_index, bool wait_for_result) {
  ASSERT_ND(block_index);
  xct::McsRwBlock* my_block = mcs_rw_blocks_ + block_index;
  if (my_block->grant_is_acked()) {
    return true;
  }
  if (wait_for_result) {
    spin_until([my_block]{ return my_block->is_waiting(); });
  } else if (my_block->is_waiting()) {
    return false;
  }

  ASSERT_ND(!my_block->is_waiting());
  auto my_tail_int = xct::McsRwLock::to_tail_int(id_, block_index);
  if (my_block->is_aborting()) {
    pass_group_tail_to_successor(my_block, my_tail_int);
    my_block->set_state_aborted();
    return false;
  } else if (my_block->is_aborted()) {
    return false;
  }
  ASSERT_ND(my_block->is_granted());

  ASSERT_ND(lock->nreaders() >= 1);
  ASSERT_ND(my_block->is_granted());
  // Take care of readers joined when I was waiting
  if (my_block->has_reader_successor()) {
    spin_until([my_block]{ return !my_block->successor_is_ready(); });
    auto* sb = get_mcs_rw_block(
      my_block->successor_thread_id_,
      my_block->successor_block_index_);
    ASSERT_ND(sb->is_reader());
    ASSERT_ND(sb->is_waiting());
    lock->increment_readers_count();
    sb->set_state_granted();
  } else if (my_block->has_writer_successor()) {
    // Bye bye writer...
    spin_until([my_block]{ return !my_block->successor_is_ready(); });
    auto* sb = get_mcs_rw_block(
      my_block->successor_thread_id_,
      my_block->successor_block_index_);
    // Clear my successor fields so we don't get confused at release_reader
    my_block->clear_successor();
    my_block->clear_successor_class();
    ASSERT_ND(sb->is_writer());
    ASSERT_ND(sb->is_waiting());
    // Prepare the successor, it will be a group leader
    auto group_tail = lock->install_tail(my_tail_int);
    ASSERT_ND(group_tail);
    ASSERT_ND(group_tail != my_tail_int);
    sb->set_group_tail_int(group_tail);
    sb->set_state_aborting();
  }
  my_block->ack_grant();
  return true;
}

void ThreadPimpl::mcs_abort_writer_lock_no_pred(
  xct::McsRwLock* mcs_rw_lock,
  xct::McsRwBlock* block,
  uint32_t tail_int) {
  ASSERT_ND(block->is_aborting());
  ASSERT_ND(block->is_writer());
  uint32_t group_tail_int = mcs_rw_lock->install_tail(0);
  if (group_tail_int != tail_int) {
    spin_until([block]{ return !block->successor_is_ready(); });
    block->set_group_tail_int(group_tail_int);
    pass_group_tail_to_successor(block, tail_int);
  }
  block->set_state_aborted();
}

bool ThreadPimpl::mcs_try_acquire_writer_lock(
  xct::McsRwLock* lock, xct::McsBlockIndex* out_block_index, int tries) {
retry:
  xct::McsBlockIndex block_index = 0;
  ASSERT_ND(out_block_index);
  if (*out_block_index) {
    // already provided, use it; caller must make sure this block is not being used
    block_index = *out_block_index;
  } else {
    block_index = *out_block_index = current_xct_.increment_mcs_block_current();
  }
  ASSERT_ND(block_index <= 0xFFFFU);
  ASSERT_ND(block_index > 0);
  xct::McsRwBlock* my_block = mcs_rw_blocks_ + block_index;
  my_block->init_writer();
  auto my_tail_int = xct::McsRwLock::to_tail_int(id_, block_index);
  my_block->pred_tail_int_ = lock->install_tail(my_tail_int);

  if (my_block->pred_tail_int_) {
    auto* pred_block = get_mcs_rw_block(my_block->pred_tail_int_);
    if (pred_block->is_writer() && pred_block->is_releasing()) {
      pred_block->set_successor_next_only(id_, block_index);
    } else {
      // Need to install pred_block's state to indicate my existence and see whether we should
      // wait for order or lead the group to abort.
      uint16_t state_desired = pred_block->make_waiting_with_writer_successor_state();
      uint16_t state_expected = pred_block->make_waiting_with_no_successor_state();
      uint16_t* state_address = &pred_block->self_.data_;
      bool ret = assorted::raw_atomic_compare_exchange_weak<uint16_t>(
        state_address, &state_expected, state_desired);
      if (ret) {
        // pred is waiting as well, wait for order (to abort most, tho)
        pred_block->set_successor_next_only(id_, block_index);
      } else {
        // Definitely pred is not waiting, if it's granted or releasing, I should proceed
        // as a group leader; if it's aborting, I should wait for order.
        bool can_get_lock = try_abort_as_group_leader(pred_block);
        if (can_get_lock) {
          ASSERT_ND(pred_block->is_releasing());
          ASSERT_ND(pred_block->successor_thread_id_ == 0);
          ASSERT_ND(pred_block->successor_block_index_ == 0);
          pred_block->set_successor_next_only(id_, block_index);
        } else {
          if (pred_block->is_aborting()) {  // wait for order
            pred_block->set_successor_next_only(id_, block_index);
          } else {
            // Decide to abort, even this is the first try
            my_block->set_state_aborting();
            ASSERT_ND(my_block->pred_tail_int_);
            auto group_tail = lock->install_tail(my_block->pred_tail_int_);
            ASSERT_ND(group_tail);
            my_block->set_group_tail_int(group_tail);
            pass_group_tail_to_successor(my_block, my_tail_int);
            my_block->set_state_aborted();
            if (--tries) {
              goto retry;
            }
            return false;  // leader can't rely on retry to give up
          }
        }
      }
    }
  }

  return true;
}

bool ThreadPimpl::mcs_retry_acquire_writer_lock(
  xct::McsRwLock* lock, xct::McsBlockIndex block_index, bool wait_for_result) {
  xct::McsRwBlock* my_block = mcs_rw_blocks_ + block_index;
  auto my_tail_int = xct::McsRwLock::to_tail_int(id_, block_index);
  ASSERT_ND(my_tail_int);
  if (my_block->grant_is_acked()) {
    ASSERT_ND(my_block->is_granted());
    return true;
  }
  if (my_block->pred_tail_int_ == 0) {
    if (lock->nreaders() == 0) {
      // No readers, got lock: nreaders() here is accurate and won't increase any more
      // after I swapped my int to the tail: readers_counter is always incremented
      // **before** setting the requesting reader's state to granted. This means if I
      // found pred=0, either there is no one or there are readers but the last the
      // preceeding readers released lock; e.g., there are readers R1->R2->R3 and tail
      // is R3. Now R3 released the lock, tail=0. Now I came and found tail=0. In this
      // case, if I found readers_count is 0, then I can be sure that there is no more
      // readers because if R3 released the lock, that means R3 got lock before, which
      // means R2 got lock before R3 did, so on until R1. R3 either got lock by finding
      // R2 is already active, or by being waken up by R2. In both cases, before R3 got
      // lock (ie set_state_granted()), we increment readers_count. So the situation
      // where a reader is already gone and the other's readers_count is just incremented
      // wont' happen.
      if (my_block->is_waiting()) {
        ASSERT_ND(my_block->is_waiting_no_pred());
        my_block->set_state_granted();
        goto acquired;
      } else if (my_block->is_aborted()) {
        return false;
      }
      ASSERT_ND(my_block->is_granted());
      ASSERT_ND(my_block->grant_is_acked());
      return true;
    }
    if (wait_for_result) {
      ASSERT_ND(my_block->is_waiting());
      ASSERT_ND(my_block->is_waiting_no_pred());
      // This is the only case where we can freely cancel - in all other cases we need
      // to wait for pred's decision, no matter wait_for_result is true or not.
      my_block->set_state_aborting();
      mcs_abort_writer_lock_no_pred(lock, my_block, my_tail_int);
      ASSERT_ND(my_block->is_aborted());
    }
    return false;
  } else {
    if (wait_for_result) {
      spin_until([my_block]{ return my_block->is_waiting(); });
    } else if (my_block->is_waiting()) {
      return false;
    }

    ASSERT_ND(!my_block->is_waiting());
    if (my_block->is_granted()) {
      ASSERT_ND(lock->nreaders() == 0);
      goto acquired;
    } else if (my_block->is_aborting()) {
      pass_group_tail_to_successor(my_block, my_tail_int);
      my_block->set_state_aborted();
      return false;
    } else if (my_block->is_aborted()) {
      return false;
    }
    ASSERT_ND(false);
  }

acquired:
  ASSERT_ND(lock->nreaders() == 0);
  if (my_block->has_writer_successor() || my_block->has_reader_successor()) {
    spin_until([my_block]{ return !my_block->successor_is_ready(); });
    auto* sb = get_mcs_rw_block(my_block->successor_thread_id_, my_block->successor_block_index_);
    ASSERT_ND(sb->is_waiting());
    my_block->clear_successor();
    my_block->clear_successor_class();
    // Prepare the successor, it will be a group leader
    auto group_tail = lock->install_tail(my_tail_int);
    ASSERT_ND(group_tail);
    ASSERT_ND(group_tail != my_tail_int);
    sb->set_group_tail_int(group_tail);
    sb->set_state_aborting();
  }
  ASSERT_ND(my_block->is_granted());
  ASSERT_ND(block_index);
  my_block->ack_grant();
  return true;
}

void ThreadPimpl::mcs_release_reader_lock(
  xct::McsRwLock* mcs_rw_lock,
  xct::McsBlockIndex block_index) {
  ASSERT_ND(block_index > 0);
  ASSERT_ND(current_xct_.get_mcs_block_current() >= block_index);
  xct::McsRwBlock* my_block = mcs_rw_blocks_ + block_index;
retry:
  uint32_t expected = xct::McsRwLock::to_tail_int(id_, block_index);
  uint32_t* tail_address = &mcs_rw_lock->tail_;
  ASSERT_ND(my_block->is_reader());
  if (!assorted::raw_atomic_compare_exchange_weak<uint32_t>(tail_address, &expected, 0)) {
    if (!(my_block->successor_is_ready() || my_block->has_aborting_successor())) {
      spin_until([my_block, mcs_rw_lock]{
        ASSERT_ND(mcs_rw_lock->nreaders());
        return !(my_block->successor_is_ready() || my_block->has_aborting_successor()); });
    }
    if (!my_block->successor_is_ready() && my_block->has_aborting_successor()) {
      goto retry;
    }
    ASSERT_ND(my_block->successor_is_ready());
  } else {
    ASSERT_ND(!my_block->successor_is_ready());
  }
  mcs_rw_lock->decrement_readers_count();
}

void ThreadPimpl::mcs_release_writer_lock(
  xct::McsRwLock* mcs_rw_lock,
  xct::McsBlockIndex block_index) {
  ASSERT_ND(block_index > 0);
  ASSERT_ND(block_index <= 0xFFFFU);
  ASSERT_ND(current_xct_.get_mcs_block_current() >= block_index);
  xct::McsRwBlock* my_block = mcs_rw_blocks_ + block_index;
  ASSERT_ND(my_block->is_writer());
  uint32_t expected = xct::McsRwLock::to_tail_int(id_, block_index);
  uint32_t* tail_address = &mcs_rw_lock->tail_;
  my_block->set_state_releasing();
retry:
  expected = xct::McsRwLock::to_tail_int(id_, block_index);
  ASSERT_ND(expected == xct::McsRwLock::to_tail_int(id_, block_index));
  if (!assorted::raw_atomic_compare_exchange_strong<uint32_t>(tail_address, &expected, 0)) {
    if (!(my_block->successor_is_ready() || my_block->has_aborting_successor())) {
      spin_until([my_block, mcs_rw_lock]{
        ASSERT_ND(mcs_rw_lock->nreaders() == 0);
        return !(my_block->successor_is_ready() || my_block->has_aborting_successor()); });
    }
    // Must check successor_is_ready first
    if (!my_block->successor_is_ready() && (my_block->has_aborting_successor())) {
      goto retry;
    }
    ASSERT_ND(my_block->successor_is_ready());
    auto* sb = get_mcs_rw_block(my_block->successor_thread_id_, my_block->successor_block_index_);
    ASSERT_ND(sb->is_waiting());
    // If sb is a reader, must increment_readers_count before set_state_granted
    if (sb->is_reader()) {
      mcs_rw_lock->increment_readers_count();
    }
    sb->set_state_granted();
  } else {
    ASSERT_ND(!my_block->successor_is_ready());
  }
}

// Returns true if the upgrade is successful and the caller should follow the writer's
// release protocol to release the lock. It's the caller's decision on what to do
// if upgrade failed.
xct::McsBlockIndex ThreadPimpl::mcs_try_upgrade_reader_lock(
  xct::McsRwLock* mcs_rw_lock,
  xct::McsBlockIndex block_index) {
ASSERT_ND(false);
  ASSERT_ND(mcs_rw_lock);
  ASSERT_ND(mcs_rw_lock->nreaders() >= 1);
  ASSERT_ND(block_index > 0);
  ASSERT_ND(current_xct_.get_mcs_block_current() >= block_index);
  xct::McsRwBlock* my_block = mcs_rw_blocks_ + block_index;
  ASSERT_ND(my_block->is_reader());
  ASSERT_ND(my_block->is_granted());

  int attempts = 0;
  // Prepare a new writer block
retry:
  xct::McsBlockIndex writer_block_index = current_xct_.increment_mcs_block_current();
  xct::McsRwBlock* writer_block = mcs_rw_blocks_ + writer_block_index;
  writer_block->init_writer();
  writer_block->set_state_granted();
  ASSERT_ND(writer_block->is_writer());
  ASSERT_ND(writer_block->is_granted());

  if (mcs_rw_lock->nreaders() == 1) {
    // Either I'm at queue tail, or somewhere in the middle; try CAS the tail from 0 or
    // myself to writer_block.
    uint32_t my_tail_int = xct::McsRwLock::to_tail_int(id_, block_index);
    uint32_t writer_tail_int = xct::McsRwLock::to_tail_int(id_, writer_block_index);
    uint32_t null_tail_int = 0;
    auto* tail_address = &mcs_rw_lock->tail_;
    if (assorted::raw_atomic_compare_exchange_strong<uint32_t>(
        tail_address,
        &my_tail_int,
        writer_tail_int) ||
      assorted::raw_atomic_compare_exchange_strong<uint32_t>(
        tail_address,
        &null_tail_int,
        writer_tail_int)) {
      // upgraded; now I'm a writer
      mcs_rw_lock->decrement_readers_count();
      return writer_block_index;
    } else {
      // free the writer block
      current_xct_.decrement_mcs_block_current();
      ASSERT_ND(mcs_rw_lock->nreaders() >= 1);
    }
  }
  if (++attempts < kLockReaderUpgradeRetries) {
    uint64_t loops = 0;
    while (++loops < kLockAcquireRetryBackoff) {}
    goto retry;
  }
  return 0;
}

static_assert(
  sizeof(ThreadControlBlock) <= soc::ThreadMemoryAnchors::kThreadMemorySize,
  "ThreadControlBlock is too large.");
}  // namespace thread
}  // namespace foedus
