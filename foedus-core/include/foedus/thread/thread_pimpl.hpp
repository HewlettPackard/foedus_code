/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_PIMPL_HPP_

#include <atomic>

#include "foedus/fixed_error_stack.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/cache/cache_hashtable.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/soc/shared_cond.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace thread {
/** Shared data of ThreadPimpl */
struct ThreadControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  ThreadControlBlock() = delete;
  ~ThreadControlBlock() = delete;

  void initialize() {
    status_ = kNotInitialized;
    mcs_block_current_ = 0;
    current_ticket_ = 0;
    proc_name_.clear();
    input_len_ = 0;
    output_len_ = 0;
    proc_result_.clear();
    wakeup_cond_.initialize();
    task_mutex_.initialize();
    task_complete_cond_.initialize();
  }
  void uninitialize() {
    task_complete_cond_.uninitialize();
    task_mutex_.uninitialize();
    wakeup_cond_.uninitialize();
  }

  /**
   * How many MCS blocks we allocated in this thread's current xct.
   * reset to 0 at each transaction begin.
   * This is in shared memory because other SOC might check this value (so far only
   * for sanity check).
   */
  uint32_t            mcs_block_current_;

  /**
   * The thread sleeps on this conditional when it has no task.
   * When someone else (whether in same SOC or other SOC) wants to wake up this logger,
   * they fire this. The 'real' condition variable is the status_.
   */
  soc::SharedCond     wakeup_cond_;

  /**
   * Impersonation status of this thread. Protected by the mutex in wakeup_cond_,
   * \b not the task_mutex_ below. Use the right mutex. Otherwise a lost signal is possible.
   */
  ThreadStatus        status_;

  /** The following variables are protected by this mutex. */
  soc::SharedMutex    task_mutex_;

  /**
   * The most recently issued impersonation ticket.
   * A session with this ticket has an exclusive ownership until it changes the status_
   * to kWaitingForTask.
   */
  ThreadTicket        current_ticket_;

  /** Name of the procedure to execute next. Empty means not set. */
  proc::ProcName      proc_name_;

  /** Byte size of input given to the procedure. */
  uint32_t            input_len_;

  /** Byte size of output as the result of the procedure. */
  uint32_t            output_len_;

  /** Error code as the result of the procedure */
  FixedErrorStack     proc_result_;

  /**
   * When the current task has been completed, the thread signals this.
   */
  soc::SharedCond     task_complete_cond_;
};

/**
 * @brief Pimpl object of Thread.
 * @ingroup THREAD
 * @details
 * A private pimpl object for Thread.
 * Do not include this header from a client program unless you know what you are doing.
 *
 * Especially, this class heavily uses C++11 classes, which is why we separate this class
 * from Thread. Be aware of notices in \ref CXX11 unless your client program allows C++11.
 */
class ThreadPimpl final : public DefaultInitializable {
 public:
  ThreadPimpl() = delete;
  ThreadPimpl(
    Engine* engine,
    Thread* holder,
    ThreadId id,
    ThreadGlobalOrdinal global_ordinal);
  ErrorStack  initialize_once() override final;
  ErrorStack  uninitialize_once() override final;

  /**
   * @brief Main routine of the worker thread.
   * @details
   * This method keeps checking current_task_. Whenever it retrieves a task, it runs
   * it and re-sets current_task_ when it's done. It exists when exit_requested_ is set.
   */
  void        handle_tasks();
  /** initializes the thread's policy/priority */
  void        set_thread_schedule();
  bool        is_stop_requested() const {
    assorted::memory_fence_acquire();
    return control_block_->status_ == kWaitingForTerminate;
  }

  /** @copydoc foedus::thread::Thread::find_or_read_a_snapshot_page() */
  ErrorCode   find_or_read_a_snapshot_page(
    storage::SnapshotPagePointer page_id,
    storage::Page** out);

  /** @copydoc foedus::thread::Thread::read_a_snapshot_page() */
  ErrorCode   read_a_snapshot_page(
    storage::SnapshotPagePointer page_id,
    storage::Page* buffer) ALWAYS_INLINE;

  /** @copydoc foedus::thread::Thread::install_a_volatile_page() */
  ErrorCode   install_a_volatile_page(
    storage::DualPagePointer* pointer,
    storage::Page** installed_page);

  /** @copydoc foedus::thread::Thread::follow_page_pointer() */
  ErrorCode   follow_page_pointer(
    storage::VolatilePageInit page_initializer,
    bool tolerate_null_pointer,
    bool will_modify,
    bool take_ptr_set_snapshot,
    bool take_ptr_set_volatile,
    storage::DualPagePointer* pointer,
    storage::Page** page,
    const storage::Page* parent,
    uint16_t index_in_parent);
  ErrorCode on_snapshot_cache_miss(
    storage::SnapshotPagePointer page_id,
    memory::PagePoolOffset* pool_offset);

  /**
   * @brief Subroutine of install_a_volatile_page() and follow_page_pointer() to atomically place
   * the given new volatile page created by this thread.
   * @param[in] new_offset offset of the new volatile page created by this thread
   * @param[in,out] pointer the address to place a new pointer.
   * @return placed_page point to the volatile page that is actually placed.
   * @details
   * Due to concurrent threads, this method might discard the given volatile page and pick
   * a page placed by another thread. In that case, new_offset will be released to the free pool.
   */
  storage::Page*  place_a_new_volatile_page(
    memory::PagePoolOffset new_offset,
    storage::DualPagePointer* pointer);


  /** @copydoc foedus::thread::Thread::collect_retired_volatile_page() */
  void          collect_retired_volatile_page(storage::VolatilePagePointer ptr);

  /**
   * Subroutine of collect_retired_volatile_page() in case the chunk becomes full.
   * Returns the chunk to volatile pool upto safe epoch. If there aren't enough pages to safely
   * return, advance the epoch (which should be very rare, tho).
   */
  void          flush_retired_volatile_page(
    uint16_t node,
    Epoch current_epoch,
    memory::PagePoolOffsetAndEpochChunk* chunk);

  /** Subroutine of collect_retired_volatile_page() just for assertion */
  bool          is_volatile_page_retired(storage::VolatilePagePointer ptr);

  /** Unconditionally takes MCS lock on the given mcs_lock. */
  xct::McsBlockIndex  mcs_acquire_lock(xct::McsLock* mcs_lock);
  /** This doesn't use any atomic operation to take a lock. only allowed when there is no race */
  xct::McsBlockIndex  mcs_initial_lock(xct::McsLock* mcs_lock);
  /** Unlcok an MCS lock acquired by this thread. */
  void                mcs_release_lock(xct::McsLock* mcs_lock, xct::McsBlockIndex block_index);
  xct::McsBlock* mcs_init_block(
    const xct::McsLock* mcs_lock,
    xct::McsBlockIndex block_index,
    bool waiting) ALWAYS_INLINE;
  void      mcs_toolong_wait(
    xct::McsLock* mcs_lock,
    ThreadId predecessor_id,
    xct::McsBlockIndex my_block,
    xct::McsBlockIndex pred_block);


  Engine* const           engine_;

  /**
   * The public object that holds this pimpl object.
   */
  Thread* const           holder_;

  /**
   * Unique ID of this thread.
   */
  const ThreadId          id_;

  /** Node this thread belongs to */
  const ThreadGroupId     numa_node_;

  /** globally and contiguously numbered ID of thread */
  const ThreadGlobalOrdinal global_ordinal_;

  /**
   * Private memory repository of this thread.
   * ThreadPimpl does NOT own it, meaning it doesn't call its initialize()/uninitialize().
   * EngineMemory owns it in terms of that.
   */
  memory::NumaCoreMemory* core_memory_;
  /** same above */
  memory::NumaNodeMemory* node_memory_;
  /** same above */
  cache::CacheHashtable*  snapshot_cache_hashtable_;
  /** shorthand for node_memory_->get_snapshot_pool() */
  memory::PagePool*       snapshot_page_pool_;

  /** Page resolver to convert all page ID to page pointer. */
  memory::GlobalVolatilePageResolver global_volatile_page_resolver_;
  /** Page resolver to convert only local page ID to page pointer. */
  memory::LocalPageResolver local_volatile_page_resolver_;

  /**
   * Thread-private log buffer.
   */
  log::ThreadLogBuffer    log_buffer_;

  /**
   * Encapsulates raw thread object.
   */
  std::thread             raw_thread_;
  /** Just to make sure raw_thread_ is set. Otherwise pthread_getschedparam will complain. */
  std::atomic<bool>       raw_thread_set_;

  /**
   * Current transaction this thread is conveying.
   * Each thread can run at most one transaction at once.
   * If this thread is not conveying any transaction, current_xct_.is_active() == false.
   */
  xct::Xct                current_xct_;

  /**
   * Each threads maintains a private set of snapshot file descriptors.
   */
  cache::SnapshotFileSet  snapshot_file_set_;

  ThreadControlBlock*     control_block_;
  void*                   task_input_memory_;
  void*                   task_output_memory_;

  /** Pre-allocated MCS blocks. index 0 is not used so that successor_block=0 means null. */
  xct::McsBlock*          mcs_blocks_;
};

inline ErrorCode ThreadPimpl::read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page* buffer) {
  return snapshot_file_set_.read_page(page_id, buffer);
}

static_assert(
  sizeof(ThreadControlBlock) <= soc::ThreadMemoryAnchors::kThreadMemorySize,
  "ThreadControlBlock is too large.");
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_PIMPL_HPP_
