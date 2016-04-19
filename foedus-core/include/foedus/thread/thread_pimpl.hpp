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
#ifndef FOEDUS_THREAD_THREAD_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_PIMPL_HPP_

#include <atomic>
#include <thread>

#include "foedus/fixed_error_stack.hpp"
#include "foedus/initializable.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/soc/shared_polling.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/thread/thread_ref.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/retrospective_lock_list.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace thread {

/** Shared data of ThreadPimpl */
struct ThreadControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  ThreadControlBlock() = delete;
  ~ThreadControlBlock() = delete;

  void initialize(ThreadId my_thread_id) {
    status_ = kNotInitialized;
    mcs_block_current_ = 0;
    mcs_rw_async_mapping_current_ = 0;
    mcs_waiting_.store(false);
    current_ticket_ = 0;
    proc_name_.clear();
    input_len_ = 0;
    output_len_ = 0;
    proc_result_.clear();
    wakeup_cond_.initialize();
    task_mutex_.initialize();
    task_complete_cond_.initialize();
    in_commit_epoch_ = INVALID_EPOCH;
    my_thread_id_ = my_thread_id;
    stat_snapshot_cache_hits_ = 0;
    stat_snapshot_cache_misses_ = 0;
  }
  void uninitialize() {
    task_mutex_.uninitialize();
  }

  /**
   * How many MCS blocks we allocated in this thread's current xct.
   * reset to 0 at each transaction begin.
   * This is in shared memory because other SOC might check this value (so far only
   * for sanity check).
   * @note So far this is a shared counter between WW and RW locks.
   * We will thus have holes in both of them. Not a big issue, but we might want
   * dedicated counters.
   */
  uint32_t            mcs_block_current_;

  /** How many async mappings for extended RW lock we have so far. */
  uint32_t            mcs_rw_async_mapping_current_;

  /**
   * Whether this thread is waiting for some MCS lock.
   * While this is true, the thread spins on this \e local variable.
   * The lock owner updates this when it unlocks.
   * We initially had this flag within each MCS lock node, but we anyway assume
   * one thread can wait for at most one lock. So, we moved it to a flag
   * in control block.
   */
  std::atomic<bool>   mcs_waiting_;

  /**
   * The thread sleeps on this conditional when it has no task.
   * When someone else (whether in same SOC or other SOC) wants to wake up this logger,
   * they fire this. The 'real' condition variable is the status_.
   */
  soc::SharedPolling  wakeup_cond_;

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
  soc::SharedPolling  task_complete_cond_;

  /** @see foedus::xct::InCommitEpochGuard  */
  Epoch               in_commit_epoch_;

  /** Used only for sanity check. This thread's ID. */
  ThreadId            my_thread_id_;

  uint64_t            stat_snapshot_cache_hits_;
  uint64_t            stat_snapshot_cache_misses_;
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
  template<typename RW_BLOCK> friend class ThreadPimplMcsAdaptor;

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
  bool        is_stop_requested() const;

  /** @copydoc foedus::thread::Thread::find_or_read_a_snapshot_page() */
  ErrorCode   find_or_read_a_snapshot_page(
    storage::SnapshotPagePointer page_id,
    storage::Page** out);
  /** @copydoc foedus::thread::Thread::find_or_read_snapshot_pages_batch() */
  ErrorCode   find_or_read_snapshot_pages_batch(
    uint16_t batch_size,
    const storage::SnapshotPagePointer* page_ids,
    storage::Page** out);

  /** @copydoc foedus::thread::Thread::read_a_snapshot_page() */
  ErrorCode   read_a_snapshot_page(
    storage::SnapshotPagePointer page_id,
    storage::Page* buffer) ALWAYS_INLINE;
  /** @copydoc foedus::thread::Thread::read_snapshot_pages() */
  ErrorCode   read_snapshot_pages(
    storage::SnapshotPagePointer page_id_begin,
    uint32_t page_count,
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
    storage::DualPagePointer* pointer,
    storage::Page** page,
    const storage::Page* parent,
    uint16_t index_in_parent);
  /** @copydoc foedus::thread::Thread::follow_page_pointers_for_read_batch() */
  ErrorCode follow_page_pointers_for_read_batch(
    uint16_t batch_size,
    storage::VolatilePageInit page_initializer,
    bool tolerate_null_pointer,
    bool take_ptr_set_snapshot,
    storage::DualPagePointer** pointers,
    storage::Page** parents,
    const uint16_t* index_in_parents,
    bool* followed_snapshots,
    storage::Page** out);
  /** @copydoc foedus::thread::Thread::follow_page_pointers_for_write_batch() */
  ErrorCode follow_page_pointers_for_write_batch(
    uint16_t batch_size,
    storage::VolatilePageInit page_initializer,
    storage::DualPagePointer** pointers,
    storage::Page** parents,
    const uint16_t* index_in_parents,
    storage::Page** out);
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

  /** */
  ThreadRef     get_thread_ref(ThreadId id);


  ////////////////////////////////////////////////////////
  /// MCS locks methods.
  /// These just delegate to xct_mcs_impl.
  /// Comments ommit as they are the same as xct_mcs_impl's.
  template<typename FUNC>
  void switch_mcs_impl(FUNC func);
  bool is_simple_mcs_rw() const { return simple_mcs_rw_; }

  /** Unconditionally takes MCS lock on the given mcs_lock. */
  xct::McsBlockIndex  mcs_acquire_lock(xct::McsWwLock* mcs_lock);
  /** Try to take MCS lock on the given mcs_lock. */
  xct::McsBlockIndex  mcs_acquire_try_lock(xct::McsWwLock* mcs_lock);
  /** This doesn't use any atomic operation to take a lock. only allowed when there is no race */
  xct::McsBlockIndex  mcs_initial_lock(xct::McsWwLock* mcs_lock);
  /** Unlcok an MCS lock acquired by this thread. */
  void                mcs_release_lock(xct::McsWwLock* mcs_lock, xct::McsBlockIndex block_index);

  xct::McsBlockIndex mcs_acquire_reader_lock(xct::McsRwLock* lock);
  xct::McsBlockIndex mcs_acquire_writer_lock(xct::McsRwLock* lock);

  /** One-shot CAS version */
  xct::McsBlockIndex mcs_try_acquire_reader_lock(xct::McsRwLock* lock);
  xct::McsBlockIndex mcs_try_acquire_writer_lock(xct::McsRwLock* lock);

  void               mcs_release_reader_lock(
    xct::McsRwLock* mcs_rw_lock,
    xct::McsBlockIndex block_index);
  void               mcs_release_writer_lock(
    xct::McsRwLock* mcs_rw_lock,
    xct::McsBlockIndex block_index);

  void        mcs_release_all_current_locks_after(xct::UniversalLockId address);
  void        mcs_giveup_all_current_locks_after(xct::UniversalLockId address);

  ErrorCode   cll_try_or_acquire_single_lock(xct::LockListPosition pos);
  ErrorCode   cll_try_or_acquire_multiple_locks(xct::LockListPosition upto_pos);
  void        cll_release_all_locks();
  xct::UniversalLockId cll_get_max_locked_id() const;


  ////////////////////////////////////////////////////////
  /// Sysxct-related.
  ErrorCode run_nested_sysxct(xct::SysxctFunctor* functor, uint32_t max_retries);
  ErrorCode sysxct_record_lock(storage::VolatilePagePointer page_id, xct::RwLockableXctId* lock);
  ErrorCode sysxct_batch_record_locks(
    storage::VolatilePagePointer page_id,
    uint32_t lock_count,
    xct::RwLockableXctId** locks);
  ErrorCode sysxct_page_lock(storage::Page* page);
  ErrorCode sysxct_batch_page_locks(uint32_t lock_count, storage::Page** pages);

  static void mcs_ownerless_acquire_lock(xct::McsWwLock* mcs_lock);
  static void mcs_ownerless_release_lock(xct::McsWwLock* mcs_lock);
  static void mcs_ownerless_initial_lock(xct::McsWwLock* mcs_lock);

  // overload to be template-friendly
  void get_mcs_rw_my_blocks(xct::McsRwSimpleBlock** out) { *out = mcs_rw_simple_blocks_; }
  void get_mcs_rw_my_blocks(xct::McsRwExtendedBlock** out) { *out = mcs_rw_extended_blocks_; }
  /// MCS locks methods.
  ////////////////////////////////////////////////////////


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
  /** shortcut for engine_->get_options().xct_.mcs_implementation_type_ == simple */
  bool                    simple_mcs_rw_;

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
  xct::McsWwBlock*          mcs_ww_blocks_;
  xct::McsRwSimpleBlock*    mcs_rw_simple_blocks_;
  xct::McsRwExtendedBlock*  mcs_rw_extended_blocks_;
  xct::McsRwAsyncMapping*   mcs_rw_async_mappings_;

  xct::RwLockableXctId*   canonical_address_;
};

/**
 * @brief Implements McsAdaptorConcept over ThreadPimpl.
 * @ingroup THREAD
 * @details
 * This object is instantiated for every MCS lock invocation in ThreadPimpl, but
 * this object is essentially just the \e this pointer itself (pimpl_).
 * The compiler will/should eliminate basically everything.
 */
template<typename RW_BLOCK>
class ThreadPimplMcsAdaptor {
 public:
  typedef RW_BLOCK ThisRwBlock;

  explicit ThreadPimplMcsAdaptor(ThreadPimpl* pimpl) : pimpl_(pimpl) {}
  ~ThreadPimplMcsAdaptor() {}

  xct::McsBlockIndex issue_new_block() { return ++pimpl_->control_block_->mcs_block_current_; }
  xct::McsBlockIndex get_cur_block() const { return pimpl_->control_block_->mcs_block_current_; }
  ThreadId      get_my_id() const { return pimpl_->id_; }
  ThreadGroupId get_my_numa_node() const { return pimpl_->numa_node_; }
  std::atomic<bool>* me_waiting() { return &pimpl_->control_block_->mcs_waiting_; }

  xct::McsWwBlock* get_ww_my_block(xct::McsBlockIndex index) {
    ASSERT_ND(index > 0);
    ASSERT_ND(index < 0xFFFFU);
    ASSERT_ND(index <= pimpl_->control_block_->mcs_block_current_);
    return pimpl_->mcs_ww_blocks_ + index;
  }
  RW_BLOCK* get_rw_my_block(xct::McsBlockIndex index) {
    ASSERT_ND(index > 0);
    ASSERT_ND(index < 0xFFFFU);
    ASSERT_ND(index <= pimpl_->control_block_->mcs_block_current_);
    RW_BLOCK* ret;
    pimpl_->get_mcs_rw_my_blocks(&ret);
    ret += index;
    return ret;
  }

  std::atomic<bool>* other_waiting(ThreadId id) {
    ThreadRef other = pimpl_->get_thread_ref(id);
    return &(other.get_control_block()->mcs_waiting_);
  }
  xct::McsBlockIndex get_other_cur_block(ThreadId id) {
    ThreadRef other = pimpl_->get_thread_ref(id);
    return other.get_control_block()->mcs_block_current_;
  }
  xct::McsWwBlock* get_ww_other_block(ThreadId id, xct::McsBlockIndex index) {
    ThreadRef other = pimpl_->get_thread_ref(id);
    ASSERT_ND(index <= other.get_control_block()->mcs_block_current_);
    return other.get_mcs_ww_blocks() + index;
  }
  RW_BLOCK* get_rw_other_block(ThreadId id, xct::McsBlockIndex index) {
    ASSERT_ND(index > 0);
    ASSERT_ND(index < 0xFFFFU);
    ThreadRef other = pimpl_->get_thread_ref(id);
    RW_BLOCK* ret;
    other.get_mcs_rw_blocks(&ret);
    ASSERT_ND(index <= other.get_control_block()->mcs_block_current_);
    ret += index;
    return ret;
  }
  RW_BLOCK* dereference_rw_tail_block(uint32_t tail_int) {
    xct::McsRwLock tail_tmp;
    tail_tmp.tail_ = tail_int;
    uint32_t tail_id = tail_tmp.get_tail_waiter();
    uint32_t tail_block = tail_tmp.get_tail_waiter_block();
    return get_rw_other_block(tail_id, tail_block);
  }
  xct::McsRwExtendedBlock* get_rw_other_async_block(ThreadId id, xct::McsRwLock* lock) {
    ASSERT_ND(id != get_my_id());
    ThreadRef other = pimpl_->get_thread_ref(id);
    assorted::memory_fence_acquire();
    auto lock_id = xct::rw_lock_to_universal_lock_id(pimpl_->global_volatile_page_resolver_, lock);
    auto* mapping = other.get_mcs_rw_async_mapping(lock_id);
    ASSERT_ND(mapping);
    ASSERT_ND(mapping->lock_id_ == lock_id);
    return get_rw_other_block(id, mapping->block_index_);
  }
  void add_rw_async_mapping(xct::McsRwLock* lock, xct::McsBlockIndex block_index) {
    // TLS, no concurrency control needed
    auto index = pimpl_->control_block_->mcs_rw_async_mapping_current_;
    ASSERT_ND(index <= pimpl_->control_block_->mcs_block_current_);
    ASSERT_ND(pimpl_->mcs_rw_async_mappings_[index].lock_id_ == xct::kNullUniversalLockId);
    ASSERT_ND(pimpl_->mcs_rw_async_mappings_[index].block_index_ == 0);
    pimpl_->mcs_rw_async_mappings_[index].lock_id_ =
      xct::rw_lock_to_universal_lock_id(pimpl_->global_volatile_page_resolver_, lock);
    pimpl_->mcs_rw_async_mappings_[index].block_index_ = block_index;
    ++pimpl_->control_block_->mcs_rw_async_mapping_current_;
  }
  void remove_rw_async_mapping(xct::McsRwLock* lock) {
    xct::UniversalLockId lock_id =
      xct::rw_lock_to_universal_lock_id(pimpl_->global_volatile_page_resolver_, lock);
    ASSERT_ND(pimpl_->control_block_->mcs_rw_async_mapping_current_);
    for (uint32_t i = 0; i < pimpl_->control_block_->mcs_rw_async_mapping_current_; ++i) {
      if (pimpl_->mcs_rw_async_mappings_[i].lock_id_ == lock_id) {
        ASSERT_ND(pimpl_->mcs_rw_async_mappings_[i].block_index_);
        pimpl_->mcs_rw_async_mappings_[i].lock_id_ = xct::kNullUniversalLockId;
        pimpl_->mcs_rw_async_mappings_[i].block_index_= 0;
        return;
      }
    }
    ASSERT_ND(false);
  }

 private:
  ThreadPimpl* const pimpl_;
};


inline ErrorCode ThreadPimpl::read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page* buffer) {
  return snapshot_file_set_.read_page(page_id, buffer);
}
inline ErrorCode ThreadPimpl::read_snapshot_pages(
  storage::SnapshotPagePointer page_id_begin,
  uint32_t page_count,
  storage::Page* buffer) {
  return snapshot_file_set_.read_pages(page_id_begin, page_count, buffer);
}

}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_PIMPL_HPP_
