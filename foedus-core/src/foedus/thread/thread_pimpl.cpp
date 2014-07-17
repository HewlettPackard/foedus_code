/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/thread_pimpl.hpp"

#include <glog/logging.h>

#include <atomic>
#include <future>
#include <mutex>
#include <thread>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/thread/impersonate_task_pimpl.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/xct/xct_inl.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace thread {
ThreadPimpl::ThreadPimpl(
  Engine* engine,
  ThreadGroupPimpl* group,
  Thread* holder,
  ThreadId id,
  ThreadGlobalOrdinal global_ordinal)
  : engine_(engine),
    group_(group),
    holder_(holder),
    id_(id),
    numa_node_(decompose_numa_node(id)),
    global_ordinal_(global_ordinal),
    core_memory_(nullptr),
    node_memory_(nullptr),
    snapshot_cache_hashtable_(nullptr),
    log_buffer_(engine, id),
    current_task_(nullptr),
    current_xct_(engine, id),
    snapshot_file_set_(engine) {
}

ErrorStack ThreadPimpl::initialize_once() {
  ASSERT_ND(engine_->get_memory_manager().is_initialized());
  core_memory_ = engine_->get_memory_manager().get_core_memory(id_);
  node_memory_ = core_memory_->get_node_memory();
  snapshot_cache_hashtable_ = node_memory_->get_snapshot_cache_table();
  current_task_ = nullptr;
  current_xct_.initialize(id_, core_memory_);
  CHECK_ERROR(snapshot_file_set_.initialize());
  CHECK_ERROR(log_buffer_.initialize());
  global_volatile_page_resolver_
    = engine_->get_memory_manager().get_global_volatile_page_resolver();
  local_volatile_page_resolver_ = node_memory_->get_volatile_pool().get_resolver();
  raw_thread_.initialize("Thread-", id_,
          std::thread(&ThreadPimpl::handle_tasks, this),
          std::chrono::milliseconds(100));
  return kRetOk;
}
ErrorStack ThreadPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  raw_thread_.stop();
  batch.emprace_back(snapshot_file_set_.uninitialize());
  batch.emprace_back(log_buffer_.uninitialize());
  core_memory_ = nullptr;
  node_memory_ = nullptr;
  snapshot_cache_hashtable_ = nullptr;
  return SUMMARIZE_ERROR_BATCH(batch);
}

void ThreadPimpl::handle_tasks() {
  int numa_node = static_cast<int>(decompose_numa_node(id_));
  LOG(INFO) << "Thread-" << id_ << " started running on NUMA node: " << numa_node;
  NumaThreadScope scope(numa_node);
  // Actual xct processing can't start until XctManager is initialized.
  SPINLOCK_WHILE(!raw_thread_.is_stop_requested()
    && !engine_->get_xct_manager().is_initialized()) {
    assorted::memory_fence_acquire();
  }
  LOG(INFO) << "Thread-" << id_ << " now starts processing transactions";
  while (!raw_thread_.sleep()) {
    VLOG(0) << "Thread-" << id_ << " woke up";
    // Keeps running if the client sets a new task immediately after this.
    while (!raw_thread_.is_stop_requested()) {
      ImpersonateTask* task = current_task_.load();
      if (task) {
        VLOG(0) << "Thread-" << id_ << " retrieved a task";
        ErrorStack result = task->run(holder_);
        VLOG(0) << "Thread-" << id_ << " run(task) returned. result =" << result;
        ASSERT_ND(current_task_.load() == task);
        current_task_.store(nullptr);  // start receiving next task
        task->pimpl_->set_result(result);  // this wakes up the client
        VLOG(0) << "Thread-" << id_ << " finished a task. result =" << result;
      } else {
        // NULL functor is the signal to terminate
        break;
      }
    }
  }
  LOG(INFO) << "Thread-" << id_ << " exits";
}

bool ThreadPimpl::try_impersonate(ImpersonateSession *session) {
  ImpersonateTask* task = nullptr;
  session->thread_ = holder_;
  if (current_task_.compare_exchange_strong(task, session->task_)) {
    // successfully acquired.
    VLOG(0) << "Impersonation succeeded for Thread-" << id_ << ".";
    raw_thread_.wakeup();
    return true;
  } else {
    // no, someone else took it.
    ASSERT_ND(task);
    session->thread_ = nullptr;
    DVLOG(0) << "Someone already took Thread-" << id_ << ".";
    return false;
  }
}

ErrorCode ThreadPimpl::install_a_volatile_page(
  storage::DualPagePointer* pointer,
  storage::Page*  volatile_parent_page,
  storage::Page** installed_page) {
  ASSERT_ND(volatile_parent_page == nullptr || !volatile_parent_page->get_header().snapshot_);
  ASSERT_ND(pointer->snapshot_pointer_ != 0);

  // copy from snapshot version
  storage::Page* snapshot_page;
  CHECK_ERROR_CODE(find_or_read_a_snapshot_page(pointer->snapshot_pointer_, &snapshot_page));
  memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
  if (UNLIKELY(offset == 0)) {
    return kErrorCodeMemoryNoFreePages;
  }
  *installed_page = local_volatile_page_resolver_.resolve_offset(offset);
  std::memcpy(*installed_page, snapshot_page, storage::kPageSize);
  // We copied from a snapshot page, so the snapshot flag is on.
  ASSERT_ND((*installed_page)->get_header().snapshot_ == false);
  // This page is a volatile page, so set the snapshot flag off and also set parent.
  (*installed_page)->get_header().snapshot_ = false;
  (*installed_page)->get_header().volatile_parent_ = volatile_parent_page;
  ASSERT_ND((*installed_page)->get_header().root_ || volatile_parent_page);
  ASSERT_ND(!(*installed_page)->get_header().root_ || volatile_parent_page == nullptr);

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
      return local_volatile_page_resolver_.resolve_offset(new_offset);
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
  const storage::VolatilePageInitializer* page_initializer,
  bool tolerate_null_pointer,
  bool will_modify,
  bool take_node_set_snapshot,
  bool take_node_set_volatile,
  storage::DualPagePointer* pointer,
  storage::Page** page) {
  return pimpl_->follow_page_pointer(
    page_initializer,
    tolerate_null_pointer,
    will_modify,
    take_node_set_snapshot,
    take_node_set_volatile,
    pointer,
    page);
}

ErrorCode ThreadPimpl::follow_page_pointer(
  const storage::VolatilePageInitializer* page_initializer,
  bool tolerate_null_pointer,
  bool will_modify,
  bool take_node_set_snapshot,
  bool take_node_set_volatile,
  storage::DualPagePointer* pointer,
  storage::Page** page) {
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
        memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
        if (UNLIKELY(offset == 0)) {
          return kErrorCodeMemoryNoFreePages;
        }
        storage::Page* new_page = local_volatile_page_resolver_.resolve_offset(offset);
        storage::VolatilePagePointer new_page_id;
        new_page_id.components.numa_node = numa_node_;
        new_page_id.components.offset = offset;
        page_initializer->initialize(new_page, new_page_id);
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
      if (!will_modify && current_xct_.get_isolation_level() == xct::kDirtyReadPreferSnapshot) {
        // even if volatile page exists. kDirtyReadPreferSnapshot prefers the snapshot page
        CHECK_ERROR_CODE(find_or_read_a_snapshot_page(pointer->snapshot_pointer_, page));
        followed_snapshot = true;
      } else {
        // otherwise (most cases) just return volatile page
        *page = global_volatile_page_resolver_.resolve_offset(volatile_pointer);
      }
    } else if (will_modify) {
      // we need a volatile page. so construct it from snapshot
      CHECK_ERROR_CODE(install_a_volatile_page(pointer, page_initializer->parent_, page));
    } else {
      // otherwise just use snapshot
      CHECK_ERROR_CODE(find_or_read_a_snapshot_page(pointer->snapshot_pointer_, page));
      followed_snapshot = true;
    }
  }

  // remember node set if needed
  if (current_xct_.get_isolation_level() == xct::kSerializable) {
    if (((*page == nullptr || followed_snapshot) && take_node_set_snapshot) ||
        (!followed_snapshot && take_node_set_volatile)) {
      current_xct_.add_to_node_set(&pointer->volatile_pointer_, volatile_pointer);
    }
  }
  return kErrorCodeOk;
}

}  // namespace thread
}  // namespace foedus
