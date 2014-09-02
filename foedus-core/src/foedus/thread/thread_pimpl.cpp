/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/thread/impersonate_task_pimpl.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
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
    snapshot_file_set_(engine),
    mcs_blocks_(nullptr) {
}

ErrorStack ThreadPimpl::initialize_once() {
  ASSERT_ND(engine_->get_memory_manager().is_initialized());
  core_memory_ = engine_->get_memory_manager().get_core_memory(id_);
  node_memory_ = core_memory_->get_node_memory();
  snapshot_cache_hashtable_ = node_memory_->get_snapshot_cache_table();
  current_task_ = nullptr;
  current_xct_.initialize(core_memory_);
  CHECK_ERROR(snapshot_file_set_.initialize());
  CHECK_ERROR(log_buffer_.initialize());
  global_volatile_page_resolver_
    = engine_->get_memory_manager().get_global_volatile_page_resolver();
  local_volatile_page_resolver_ = node_memory_->get_volatile_pool().get_resolver();
  mcs_blocks_ = reinterpret_cast<McsBlock*>(
    core_memory_->get_small_thread_local_memory_pieces().thread_mcs_block_memory_);
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

void ThreadPimpl::hack_handle_one_task(ImpersonateTask* task, ImpersonateSession* session) {
  NumaThreadScope scope(numa_node_);
  session->thread_ = holder_;
  session->task_ = task;
  ErrorStack result = task->run(holder_);
  task->pimpl_->set_result(result);
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
  set_thread_schedule();
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
void ThreadPimpl::set_thread_schedule() {
  // this code totally assumes pthread. maybe ifdef to handle Windows.. later!
  pthread_t handle = raw_thread_.native_handle<pthread_t>();
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
  storage::Page** installed_page) {
  ASSERT_ND(pointer->snapshot_pointer_ != 0);

  // copy from snapshot version
  storage::Page* snapshot_page;
  CHECK_ERROR_CODE(find_or_read_a_snapshot_page(pointer->snapshot_pointer_, &snapshot_page));
  memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
  if (UNLIKELY(offset == 0)) {
    return kErrorCodeMemoryNoFreePages;
  }
  *installed_page = local_volatile_page_resolver_.resolve_offset_newpage(offset);
  std::memcpy(*installed_page, snapshot_page, storage::kPageSize);
  // We copied from a snapshot page, so the snapshot flag is on.
  ASSERT_ND((*installed_page)->get_header().snapshot_ == false);
  // This page is a volatile page, so set the snapshot flag off.
  (*installed_page)->get_header().snapshot_ = false;

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
  const storage::VolatilePageInitializer* page_initializer,
  bool tolerate_null_pointer,
  bool will_modify,
  bool take_ptr_set_snapshot,
  bool take_ptr_set_volatile,
  storage::DualPagePointer* pointer,
  storage::Page** page) {
  return pimpl_->follow_page_pointer(
    page_initializer,
    tolerate_null_pointer,
    will_modify,
    take_ptr_set_snapshot,
    take_ptr_set_volatile,
    pointer,
    page);
}

ErrorCode ThreadPimpl::follow_page_pointer(
  const storage::VolatilePageInitializer* page_initializer,
  bool tolerate_null_pointer,
  bool will_modify,
  bool take_ptr_set_snapshot,
  bool take_ptr_set_volatile,
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
        ASSERT_ND(page_initializer != &(storage::kDummyPageInitializer));
        memory::PagePoolOffset offset = core_memory_->grab_free_volatile_page();
        if (UNLIKELY(offset == 0)) {
          return kErrorCodeMemoryNoFreePages;
        }
        storage::Page* new_page = local_volatile_page_resolver_.resolve_offset_newpage(offset);
        storage::VolatilePagePointer new_page_id;
        new_page_id.components.numa_node = numa_node_;
        new_page_id.components.offset = offset;
        page_initializer->initialize(new_page, new_page_id);
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
      CHECK_ERROR_CODE(install_a_volatile_page(pointer, page));
    } else {
      // otherwise just use snapshot
      CHECK_ERROR_CODE(find_or_read_a_snapshot_page(pointer->snapshot_pointer_, page));
      followed_snapshot = true;
    }
  }

  // if we follow a snapshot pointer, remember pointer set
  if (current_xct_.get_isolation_level() == xct::kSerializable) {
    if (((*page == nullptr || followed_snapshot) && take_ptr_set_snapshot) ||
        (!followed_snapshot && take_ptr_set_volatile)) {
      current_xct_.add_to_pointer_set(&pointer->volatile_pointer_, volatile_pointer);
    }
  }
  return kErrorCodeOk;
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

inline ThreadPimpl::McsBlock* ThreadPimpl::mcs_init_block(
  const xct::McsLock* mcs_lock,
  xct::McsBlockIndex block_index,
  bool waiting) {
  ASSERT_ND(block_index > 0);
  McsBlock* block = mcs_blocks_ + block_index;
  block->waiting_ = waiting;
  block->lock_addr_tag_ = mcs_lock->last_1byte_addr();
  block->successor_ = 0;
  block->successor_block_ = 0;
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
  McsBlock* block = mcs_init_block(mcs_lock, block_index, true);
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
  xct::McsBlockIndex predecessor_block = old.get_tail_waiter_block();
  DVLOG(0) << "mm, contended, we have to wait.. me=" << id_ << " pred=" << predecessor_id;
  ASSERT_ND(decompose_numa_node(predecessor_id) < engine_->get_options().thread_.group_count_);
  ASSERT_ND(decompose_numa_local_ordinal(predecessor_id) <
    engine_->get_options().thread_.thread_count_per_group_);
  ThreadPimpl* predecessor
    = engine_->get_thread_pool().get_pimpl()->get_thread(predecessor_id)->get_pimpl();
  ASSERT_ND(predecessor);
  ASSERT_ND(predecessor != this);
  ASSERT_ND(block->waiting_);
  // TODO(Hideaki) because of tentative SOC implementation. we can't check this
  // ASSERT_ND(predecessor->current_xct_.get_mcs_block_current() >= predecessor_block);
  McsBlock* pred_block = predecessor->mcs_blocks_ + predecessor_block;
  ASSERT_ND(pred_block->successor_ == 0);
  ASSERT_ND(pred_block->successor_block_ == 0);
  ASSERT_ND(pred_block->lock_addr_tag_ == block->lock_addr_tag_);

  pred_block->successor_ = id_;
  assorted::memory_fence_release();  // set successor_, then successor_block_
  pred_block->successor_block_ = block_index;
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
  McsBlock* block = mcs_blocks_ + block_index;
  ASSERT_ND(!block->waiting_);
  ASSERT_ND(block->lock_addr_tag_ == mcs_lock->last_1byte_addr());
  if (block->successor_block_ == 0) {
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
      // so we can't put assertion here.
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
    while (block->successor_block_ == 0) {
      ASSERT_ND(mcs_lock->is_locked());
      if (((++spins) & 0xFFFFFFU) == 0) {
        assorted::spinlock_yield();
      }
      assorted::memory_fence_acquire();
      continue;
    }
  }
  DVLOG(1) << "Okay, I have a successor. me=" << id_ << ", succ=" << block->successor_;
  ASSERT_ND(block->successor_ != id_);

  ThreadPimpl* successor
    = engine_->get_thread_pool().get_pimpl()->get_thread(block->successor_)->get_pimpl();
  // TODO(Hideaki) because of tentative SOC implementation. we can't check this
  // ASSERT_ND(successor->current_xct_.get_mcs_block_current() >= block->successor_block_);
  McsBlock* succ_block = successor->mcs_blocks_ + block->successor_block_;
  ASSERT_ND(succ_block->lock_addr_tag_ == mcs_lock->last_1byte_addr());
  ASSERT_ND(succ_block->waiting_);
  ASSERT_ND(mcs_lock->is_locked());
  assorted::memory_fence_acq_rel();
  succ_block->waiting_ = false;
  assorted::memory_fence_acq_rel();
}



}  // namespace thread
}  // namespace foedus
