/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_HPP_
#define FOEDUS_THREAD_THREAD_HPP_
#include <iosfwd>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace thread {
/**
 * @brief Represents one thread running on one NUMA core.
 * @ingroup THREAD
 * @details
 */
class Thread CXX11_FINAL : public virtual Initializable {
 public:
  Thread() CXX11_FUNC_DELETE;
  Thread(Engine* engine, ThreadGroupPimpl* group, ThreadId id, ThreadGlobalOrdinal global_ordinal);
  ~Thread();
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  Engine*     get_engine() const;
  ThreadId    get_thread_id() const;
  ThreadGroupId get_numa_node() const { return decompose_numa_node(get_thread_id()); }
  ThreadGlobalOrdinal get_thread_global_ordinal() const;

  /**
   * Returns the transaction that is currently running on this thread.
   */
  xct::Xct&   get_current_xct();
  /** Returns if this thread is running an active transaction. */
  bool        is_running_xct() const;

  /** Returns the private memory repository of this thread. */
  memory::NumaCoreMemory* get_thread_memory() const;
  /** Returns the node-shared memory repository of the NUMA node this thread belongs to. */
  memory::NumaNodeMemory* get_node_memory() const;

  /**
   * @brief Returns the private log buffer for this thread.
   */
  log::ThreadLogBuffer&   get_thread_log_buffer();

  /**
   * Returns the page resolver to convert page ID to page pointer.
   * All worker threads copy the page resolver into its local memory at startup.
   * This gives the most efficient page resolve without any remote NUMA memory access.
   */
  const memory::GlobalVolatilePageResolver& get_global_volatile_page_resolver() const {
    return global_volatile_page_resolver_;
  }
  /** Returns page resolver to convert only local page ID to page pointer. */
  const memory::LocalPageResolver& get_local_volatile_page_resolver() const;

  /**
   * Find the given page in snapshot cache, reading it if not found.
   */
  ErrorCode     find_or_read_a_snapshot_page(
    storage::SnapshotPagePointer page_id,
    storage::Page** out);

  /**
   * Read a snapshot page using the thread-local file descriptor set.
   * @attention this method always READs, so no caching done. Actually, this method is used
   * from caching module when cache miss happens. To utilize cache,
   * use find_or_read_a_snapshot_page().
   */
  ErrorCode     read_a_snapshot_page(storage::SnapshotPagePointer page_id, storage::Page* buffer);

  /**
   * @brief Installs a volatile page to the given dual pointer as a copy of the snapshot page.
   * @param[in,out] pointer dual pointer. volatile pointer will be modified.
   * @param[in] parent_volatile_page Volatile pages maintain parent pointers. This is set
   * as the parent of the new volatile page.
   * @param[out] installed_page physical pointer to the installed volatile page. This might point
   * to a page installed by a concurrent thread.
   * @pre pointer->snapshot_pointer_ != 0 (this method is for a page that already has snapshot)
   * @pre pointer->volatile_pointer.components.offset == 0 (but not mandatory because
   * concurrent threads might have installed it right now)
   * @details
   * This is called when a dual pointer has only a snapshot pointer, in other words it is "clean",
   * to create a volatile version for modification.
   */
  ErrorCode     install_a_volatile_page(
    storage::DualPagePointer* pointer,
    storage::Page*  parent_volatile_page,
    storage::Page** installed_page);

  /**
   * @brief A general method to follow (read) a page pointer.
   * @param[in] page_initializer callback object in case we need to initialize a new volatile page.
   * @param[in] tolerate_null_pointer when true and when both the volatile and snapshot pointers
   * seem null, we return null page rather than creating a new volatile page.
   * @param[in] will_modify if true, we always return a non-null volatile page. This is true
   * when we are to modify the page, such as insert/delete.
   * @param[in] take_node_set_snapshot if true, we add the address of volatile page pointer
   * to node set when we do not follow a volatile pointer (null or volatile). This is usually true
   * to make sure we get aware of new page installment by concurrent threads.
   * If the isolation level is not serializable, we don't take node set anyways.
   * @param[in] take_node_set_volatile if true, we add the address of volatile page pointer
   * to node set even when we follow a volatile pointer. This is true only when the storage
   * might have RCU-style page switching (eg Masstree).
   * If the isolation level is not serializable, we don't take node set anyways.
   * @param[in,out] pointer the page pointer.
   * @param[out] page the read page.
   * @pre !tolerate_null_pointer || !will_modify (if we are modifying the page, tolerating null
   * pointer doesn't make sense. we should always initialize a new volatile page)
   * @details
   * This is the primary way to retrieve a page pointed by a pointer in various places.
   * Depending on the current transaction's isolation level and storage type (represented by
   * the various arguments), this does a whole lots of things to comply with our commit protocol.
   *
   * Remember that DualPagePointer maintains volatile and snapshot pointers.
   * We sometimes have to install a new volatile page or add the pointer to node set
   * for serializability. That logic is a bit too lengthy method to duplicate in each page
   * type, so generalize it here.
   */
  ErrorCode     follow_page_pointer(
    const storage::VolatilePageInitializer* page_initializer,
    bool tolerate_null_pointer,
    bool will_modify,
    bool take_node_set_snapshot,
    bool take_node_set_volatile,
    storage::DualPagePointer* pointer,
    storage::Page** page);

  /** Returns the pimpl of this object. Use it only when you know what you are doing. */
  ThreadPimpl*  get_pimpl() const { return pimpl_; }

  friend std::ostream& operator<<(std::ostream& o, const Thread& v);

 private:
  /**
   * The page resolver to convert page ID to page pointer (a copy of the object in EngineMemory).
   * As an exception to pimpl idiom, this object is allocated in Thread itself.
   * This is because the page resolve is such a freuquently used object that must have
   * a really low overhead to retrieve (in other words, in-lined).
   * Fortunately, it has no dependency, so containing this object wouldn't cause an issue.
   */
  memory::GlobalVolatilePageResolver  global_volatile_page_resolver_;

  ThreadPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_HPP_
