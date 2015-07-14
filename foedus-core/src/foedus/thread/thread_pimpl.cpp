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
    mcs_blocks_(nullptr) {
}

ErrorStack ThreadPimpl::initialize_once() {
  ASSERT_ND(engine_->get_memory_manager()->is_initialized());

  soc::ThreadMemoryAnchors* anchors
    = engine_->get_soc_manager()->get_shared_memory_repo()->get_thread_memory_anchors(id_);
  control_block_ = anchors->thread_memory_;
  control_block_->initialize();
  task_input_memory_ = anchors->task_input_memory_;
  task_output_memory_ = anchors->task_output_memory_;
  mcs_blocks_ = anchors->mcs_lock_memories_;

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

inline void assert_mcs_aligned(const void* address) {
  ASSERT_ND(address);
  ASSERT_ND(reinterpret_cast<uintptr_t>(address) % 4 == 0);
}

inline xct::McsBlock* ThreadPimpl::mcs_init_block(
  const xct::McsLock* mcs_lock,
  xct::McsBlockIndex block_index,
  bool waiting) {
  ASSERT_ND(block_index > 0);
  xct::McsBlock* block = mcs_blocks_ + block_index;
  block->waiting_ = waiting;
  block->lock_addr_tag_ = mcs_lock->last_1byte_addr();
  block->clear_successor();
  return block;
}

void ThreadPimpl::mcs_toolong_wait(
  xct::McsLock* mcs_lock,
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
  assorted::memory_fence_acq_rel();
  assert_mcs_aligned(mcs_lock);
  // so far we allow only 2^16 MCS blocks per transaction. we might increase later.
  ASSERT_ND(current_xct_.get_mcs_block_current() < 0xFFFFU);
  xct::McsBlockIndex block_index = current_xct_.increment_mcs_block_current();
  ASSERT_ND(block_index > 0);
  xct::McsBlock* block = mcs_init_block(mcs_lock, block_index, true);
  uint32_t desired = xct::McsLock::to_int(id_, block_index);
  uint32_t* address = &(mcs_lock->data_);
  assert_mcs_aligned(address);

  // atomic op should imply full barrier, but make sure announcing the initialized new block.
  assorted::memory_fence_release();  // https://bugzilla.mozilla.org/show_bug.cgi?id=873799
#if defined(__GNUC__)
  // GCC's builtin atomic. maybe a bit faster
  uint32_t old_int = __sync_lock_test_and_set(address, desired);
#else  // defined(__GNUC__)
  uint32_t old_int = assorted::raw_atomic_exchange<uint32_t>(address, desired);
#endif  // defined(__GNUC__)

  xct::McsLock old;
  old.data_ = old_int;
  if (!old.is_locked()) {
    // this means it was not locked.
    ASSERT_ND(mcs_lock->is_locked());
    DVLOG(2) << "Okay, got a lock uncontended. me=" << id_;
    block->waiting_ = false;
    // atomic op should imply full barrier, but make sure
    assorted::memory_fence_acq_rel();
    return block_index;
  }

  ASSERT_ND(mcs_lock->is_locked());
  ThreadId predecessor_id = old.get_tail_waiter();
  ASSERT_ND(predecessor_id != id_);
  xct::McsBlockIndex predecessor_block = old.get_tail_waiter_block();
  DVLOG(0) << "mm, contended, we have to wait.. me=" << id_ << " pred=" << predecessor_id;
  ASSERT_ND(decompose_numa_node(predecessor_id) < engine_->get_options().thread_.group_count_);
  ASSERT_ND(decompose_numa_local_ordinal(predecessor_id) <
    engine_->get_options().thread_.thread_count_per_group_);

  ThreadRef* predecessor = pool_pimpl_->get_thread(predecessor_id);
  ASSERT_ND(predecessor);
  ASSERT_ND(block->waiting_);
  ASSERT_ND(predecessor->get_control_block()->mcs_block_current_ >= predecessor_block);
  xct::McsBlock* pred_block = predecessor->get_mcs_blocks() + predecessor_block;
  ASSERT_ND(pred_block->get_successor_thread_id() == 0);
  ASSERT_ND(!pred_block->has_successor());
  ASSERT_ND(pred_block->lock_addr_tag_ == block->lock_addr_tag_);

  pred_block->set_successor(id_, block_index);
  assorted::memory_fence_release();

  // spin locally
  uint64_t spins = 0;
  while (block->waiting_) {
    ASSERT_ND(mcs_lock->is_locked());
    assorted::memory_fence_acquire();
    if (((++spins) & 0xFFFFFFU) == 0) {
      assorted::spinlock_yield();
    }
    /*
    if (spins == 0x10000000U) {
      // Probably fixed the root cause (GCC's union handling).. but let's leave it here.
      // gggrr, I get the deadlock only when I do not put this here.
      // wtf. gcc bug or my brain is dead. let's figure out later
      mcs_toolong_wait(mcs_lock, predecessor_id, block_index, predecessor_block);
    }
    // NO, if I enable this harmful code, now it starts again. WWWWWTTTTTTTTTTFFFFFFFFFF
    */
    continue;
  }
  DVLOG(1) << "Okay, now I hold the lock. me=" << id_ << ", ex-pred=" << predecessor_id;
  ASSERT_ND(!block->waiting_);
  ASSERT_ND(mcs_lock->is_locked());
  assorted::memory_fence_acq_rel();
  return block_index;
}

xct::McsBlockIndex ThreadPimpl::mcs_initial_lock(xct::McsLock* mcs_lock) {
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(!mcs_lock->is_locked());
  // so far we allow only 2^16 MCS blocks per transaction. we might increase later.
  ASSERT_ND(current_xct_.get_mcs_block_current() < 0xFFFFU);
  xct::McsBlockIndex block_index = current_xct_.increment_mcs_block_current();
  ASSERT_ND(block_index > 0);
  mcs_init_block(mcs_lock, block_index, false);
  mcs_lock->reset(id_, block_index);
  assorted::memory_fence_acq_rel();
  return block_index;
}

void ThreadPimpl::mcs_release_lock(xct::McsLock* mcs_lock, xct::McsBlockIndex block_index) {
  assorted::memory_fence_acq_rel();
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(mcs_lock->is_locked());
  ASSERT_ND(block_index > 0);
  ASSERT_ND(current_xct_.get_mcs_block_current() >= block_index);
  xct::McsBlock* block = mcs_blocks_ + block_index;
  ASSERT_ND(!block->waiting_);
  ASSERT_ND(block->lock_addr_tag_ == mcs_lock->last_1byte_addr());
  if (!block->has_successor()) {
    // okay, successor "seems" nullptr (not contended), but we have to make it sure with atomic CAS
    uint32_t expected = xct::McsLock::to_int(id_, block_index);
    uint32_t* address = &(mcs_lock->data_);
    assert_mcs_aligned(address);
#if defined(__GNUC__)
    // GCC's builtin atomic. maybe a bit faster because we don't have to give an address of expected
    bool swapped = __sync_bool_compare_and_swap(address, expected, 0);
#else  // defined(__GNUC__)
    bool swapped = assorted::raw_atomic_compare_exchange_strong<uint32_t>(address, &expected, 0);
#endif  // defined(__GNUC__)
    if (swapped) {
      // we have just unset the locked flag, but someone else might have just acquired it,
      // so we can't put assertion (eg !mcs_lock->is_locked()) here, but at least it's not me.
      ASSERT_ND(id_ == 0 || mcs_lock->get_tail_waiter() != id_);
      DVLOG(2) << "Okay, release a lock uncontended. me=" << id_;
      assorted::memory_fence_acq_rel();
      return;
    }
    DVLOG(0) << "Interesting contention on MCS release. I thought it's null, but someone has just "
      " jumped in. me=" << id_ << ", mcs_lock=" << *mcs_lock;
    // wait for someone else to set the successor
    ASSERT_ND(mcs_lock->is_locked());
    uint64_t spins = 0;
    while (!block->has_successor()) {
      ASSERT_ND(mcs_lock->is_locked());
      if (((++spins) & 0xFFFFFFU) == 0) {
        assorted::spinlock_yield();
      }
      assorted::memory_fence_acquire();
      continue;
    }
  }

  ASSERT_ND(block->has_successor());
  thread::ThreadId successor_id = block->get_successor_thread_id();
  xct::McsBlockIndex successor_block = block->get_successor_block();
  DVLOG(1) << "Okay, I have a successor. me=" << id_ << ", succ="
    << successor_id << ", succ_block=" << successor_block;
  ASSERT_ND(successor_id != id_);

  ThreadRef* successor = pool_pimpl_->get_thread(successor_id);
  ASSERT_ND(successor->get_control_block()->mcs_block_current_ >= successor_block);
  xct::McsBlock* succ_block = successor->get_mcs_blocks() + successor_block;
  ASSERT_ND(succ_block->lock_addr_tag_ == mcs_lock->last_1byte_addr());
  ASSERT_ND(succ_block->waiting_);
  ASSERT_ND(mcs_lock->is_locked());
  succ_block->waiting_ = false;
  assorted::memory_fence_acq_rel();
}


static_assert(
  sizeof(ThreadControlBlock) <= soc::ThreadMemoryAnchors::kThreadMemorySize,
  "ThreadControlBlock is too large.");

static_assert(
  sizeof(xct::McsBlock) * (1U << 16) <= soc::ThreadMemoryAnchors::kMcsBlockMemorySize,
  "kMcsBlockMemorySize is too small.");

}  // namespace thread
}  // namespace foedus
