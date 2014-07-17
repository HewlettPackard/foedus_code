/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_PIMPL_HPP_
#include <atomic>

#include "foedus/initializable.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/cache/cache_hashtable.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"
#include "foedus/xct/xct.hpp"

namespace foedus {
namespace thread {
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
    ThreadGroupPimpl* group,
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

  /**
   * Conditionally try to occupy this thread, or impersonate. If it fails, it immediately returns.
   * @param[in] session the session to run on this thread
   * @return whether successfully impersonated.
   */
  bool        try_impersonate(ImpersonateSession *session);

  /** @copydoc foedus::thread::Thread::find_or_read_a_snapshot_page() */
  ErrorCode   find_or_read_a_snapshot_page(
    storage::SnapshotPagePointer page_id,
    storage::Page** out) ALWAYS_INLINE;

  /** @copydoc foedus::thread::Thread::read_a_snapshot_page() */
  ErrorCode   read_a_snapshot_page(
    storage::SnapshotPagePointer page_id,
    storage::Page* buffer) ALWAYS_INLINE;

  /** @copydoc foedus::thread::Thread::install_a_volatile_page() */
  ErrorCode   install_a_volatile_page(
    storage::DualPagePointer* pointer,
    storage::Page*  parent_volatile_page,
    storage::Page** installed_page);

  /** @copydoc foedus::thread::Thread::follow_page_pointer() */
  ErrorCode   follow_page_pointer(
    const storage::VolatilePageInitializer* page_initializer,
    bool tolerate_null_pointer,
    bool will_modify,
    bool take_node_set_snapshot,
    bool take_node_set_volatile,
    storage::DualPagePointer* pointer,
    storage::Page** page);

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

  Engine* const           engine_;

  /**
   * The thread group (NUMA node) this thread belongs to.
   */
  ThreadGroupPimpl* const group_;

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
   * This is initialized/uninitialized in initialize()/uninitialize().
   */
  StoppableThread         raw_thread_;

  /**
   * The task this thread is currently running or will run when it wakes up.
   * Only one caller can impersonate a thread at once.
   * If this thread is not impersonated, null.
   */
  std::atomic<ImpersonateTask*>   current_task_;

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
};

inline ErrorCode ThreadPimpl::read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page* buffer) {
  return snapshot_file_set_.read_page(page_id, buffer);
}

inline ErrorCode ThreadPimpl::find_or_read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page** out) {
  return snapshot_cache_hashtable_->read_page(page_id, holder_, out);
}

}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_PIMPL_HPP_
