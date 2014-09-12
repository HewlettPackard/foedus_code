/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_
#define FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_

#include <stdint.h>

#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/shared_memory.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/soc/fwd.hpp"
#include "foedus/soc/soc_id.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace soc {

/**
 * @brief Current status of master engine.
 * @ingroup SOC
 * @details
 * This object is placed in shared memory, so this must not have a heap-allocated member.
 */
struct MasterEngineStatus CXX11_FINAL {
  /**
   * These statuses represent each step described in SocManager comment.
   * The status always increases one-by-one or jumps to kFatalError.
   */
  enum StatusCode {
    /**
     * Master engine has just started.
     */
    kInitial = 0,
    /**
     * Master engine successfully allocated shared memory and waiting for child's attach.
     */
    kSharedMemoryAllocated,
    /**
     * Master engine successfully confirmed child's attach and reserved the reclamation of
     * the shared memories. No more attach to the memories is possible after this.
     */
    kSharedMemoryReservedReclamation,
    /**
     * Master engine has successfully initialized the debug module.
     * Child SOCs resume their initialization after observing this status.
     * This additional synchronization is because of the fork-related issue in glog.
     * If SOC type is not fork, we can omit this step, but wouldn't matter.
     */
    kDebugModuleInitialized,

    /**
     * The master is waiting for child engines to complete their initialization.
     */
    kWaitingForChildInitialization,

    /**
     * The master engine is finishing up initialization, such as restart module.
     */
    kInitializingRest,

    /** Done all initialization and running transactions. */
    kRunning,

    /**
     * The master is waiting for child engines to terminate.
     * As soon as child observes this, they will start termination in normal steps.
     */
    kWaitingForChildTerminate,
    /** The master engine has normally terminated. This is a final status. */
    kTerminated,
    /** Whenever child observes this, they will call _exit() asap. This is a final status. */
    kFatalError,
  };

  // only for reinterpret_cast
  MasterEngineStatus() CXX11_FUNC_DELETE;
  ~MasterEngineStatus() CXX11_FUNC_DELETE;
  MasterEngineStatus(const MasterEngineStatus &other) CXX11_FUNC_DELETE;
  MasterEngineStatus& operator=(const MasterEngineStatus &other) CXX11_FUNC_DELETE;

  /** Update the value of status_code_ with fence */
  void change_status_atomic(StatusCode new_status) {
    ASSERT_ND(new_status == kFatalError ||
      static_cast<int>(new_status) == static_cast<int>(status_code_) + 1);
    assorted::memory_fence_acq_rel();
    status_code_ = new_status;
    assorted::memory_fence_acq_rel();
  }
  /** Read status_code_ with fence. */
  StatusCode read_status_atomic() const {
    assorted::memory_fence_acq_rel();
    return status_code_;
  }

  StatusCode status_code_;
  // so far this is the only information.
};
CXX11_STATIC_ASSERT(
  sizeof(MasterEngineStatus) <= (1 << 12),
  "Size of MasterEngineStatus exceeds 4kb");

/**
 * @brief Current status of a child SOC engine.
 * @ingroup SOC
 * @details
 * This object is placed in shared memory, so this must not have a heap-allocated member.
 */
struct ChildEngineStatus CXX11_FINAL {
  /**
   * These statuses represent each step described in SocManager comment.
   * The status always increases one-by-one or jumps to kFatalError.
   */
  enum StatusCode {
    /**
     * Child engine has just started.
     */
    kInitial = 0,
    /**
     * Child engine successfully attached shared memory and waiting for master's
     * kDebugModuleInitialized status.
     */
    kSharedMemoryAttached,
    /**
     * Child engine has successfully initialized all modules and is now waiting for master's
     * kRunning status.
     */
    kWaitingForMasterInitialization,

    /** Done all initialization and running transactions. */
    kRunning,

    /** The child engine has normally terminated. This is a final status. */
    kTerminated,
    /** The child engine observed some unrecoverable error and has exit. This is a final status. */
    kFatalError,
  };

  // only for reinterpret_cast
  ChildEngineStatus() CXX11_FUNC_DELETE;
  ~ChildEngineStatus() CXX11_FUNC_DELETE;
  ChildEngineStatus(const ChildEngineStatus &other) CXX11_FUNC_DELETE;
  ChildEngineStatus& operator=(const ChildEngineStatus &other) CXX11_FUNC_DELETE;

  /** Update the value of status_code_ with fence */
  void change_status_atomic(StatusCode new_status) {
    ASSERT_ND(new_status == kFatalError ||
      static_cast<int>(new_status) == static_cast<int>(status_code_) + 1);
    assorted::memory_fence_acq_rel();
    status_code_ = new_status;
    assorted::memory_fence_acq_rel();
  }
  /** Read status_code_ with fence. */
  StatusCode read_status_atomic() const {
    assorted::memory_fence_acq_rel();
    return status_code_;
  }

  StatusCode status_code_;
  // so far this is the only information.
};

CXX11_STATIC_ASSERT(
  sizeof(ChildEngineStatus) <= (1 << 12),
  "Size of ChildEngineStatus exceeds 4kb");

/**
  * Just a set of pointers within global_memory_ for ease of use.
  * All pointers are at least 8-byte aligned. Most of them are 4kb aligned.
  */
struct GlobalMemoryAnchors {
  enum Constants {
    kMasterStatusMemorySize = 1 << 12,
    kEpochStatusMemorySize = 1 << 12,
    kStorageManagerStatusMemorySize = 1 << 12,
    kStorageStatusSizePerStorage = 1 << 12,
  };

  GlobalMemoryAnchors() { clear(); }
  ~GlobalMemoryAnchors() {}
  void clear() { std::memset(this, 0, sizeof(*this)); }

  // No copying
  GlobalMemoryAnchors(const GlobalMemoryAnchors &other) CXX11_FUNC_DELETE;
  GlobalMemoryAnchors& operator=(const GlobalMemoryAnchors &other) CXX11_FUNC_DELETE;

  /** The beginning of global memory is an XML-serialized EngineOption. The byte size. */
  uint64_t  options_xml_length_;
  /** The xml itself. not null terminated. */
  char*     options_xml_;

  /**
   * This tiny piece of memory contains the current status of the master engine and
   * its synchronization mechanisms. Always 4kb.
   */
  MasterEngineStatus* master_status_memory_;

  /**
   * This tiny piece of memory contains the value of current/durable epochs, snapshot/savepoint,
   * and their synchronization mechanisms. Always 4kb.
   */
  void*     epoch_status_memory_;

  /**
   * This memory stores the number of storages and synchronization mechanisms
   * for storage manager.
   * Always 4kb.
   */
  void*     storage_manager_status_memory_;

  /**
   * This memory stores the ID of storages sorted by their names.
   * The size is 4 (=sizeof(StorageId)) * StorageOptions::max_storages_.
   */
  storage::StorageId* storage_name_sort_memory_;

  /**
   * Status of each storage instance is stored in this shared memory.
   * The size for one storage must be within 4kb. If the storage type requires more than 4kb,
   * just grab a page from volatile page pools and point to it from the storage object.
   * The size is 4kb * StorageOptions::max_storages_.
   */
  void*     storage_status_memory_;
};

/**
 * Same as GlobalMemoryAnchors except this is for node_memories_.
 */
struct NodeMemoryAnchors {
  enum Constants {
    kChildStatusMemorySize = 1 << 12,
    kVolatilePoolStatusMemorySize = 1 << 12,
    kLoggerStatusMemorySize = 1 << 12,
    kProcManagerStatusMemorySize = 1 << 12,
  };

  NodeMemoryAnchors() { std::memset(this, 0, sizeof(*this)); }
  ~NodeMemoryAnchors() { deallocate_arrays(); }
  // No copying
  NodeMemoryAnchors(const NodeMemoryAnchors &other) CXX11_FUNC_DELETE;
  NodeMemoryAnchors& operator=(const NodeMemoryAnchors &other) CXX11_FUNC_DELETE;

  void allocate_arrays(const EngineOptions& options);
  void deallocate_arrays();

  /**
   * This tiny piece of memory contains the current status of the child engine on this node.
   * Always 4kb.
   */
  ChildEngineStatus*  child_status_memory_;

  /**
   * PagePool's status and its synchronization mechanism for the volatile pool on this node.
   * Always 4kb.
   */
  void*               volatile_pool_status_memory_;

  /**
   * ProcManagers's status and its synchronization mechanism on this node.
   * Always 4kb.
   */
  void*               proc_manager_status_memory_;
  /**
   * Procedure list on this node.
   * The size is sizeof(proc::ProcAndName) * ProcOptions::max_proc_count_.
   */
  proc::ProcAndName*  proc_memory_;
  /**
   * This memory stores the ID of procedures sorted by their names.
   * The size is 4 (=sizeof(LocalProcId)) * ProcOptions::max_proc_count_.
   */
  proc::LocalProcId*  proc_name_sort_memory_;

  /**
   * Status and synchronization mechanism for loggers on this node.
   * Index is node-local logger ID.
   * 4kb for each logger.
   */
  void**              logger_status_memories_;

  /**
   * Anchors for each thread. Index is node-local thread ordinal.
   */
  ThreadMemoryAnchors*  thread_anchors_;
};

/**
 * Part of NodeMemoryAnchors for each thread.
 */
struct ThreadMemoryAnchors {
  enum Constants {
    kImpersonateStatusMemorySize = 1 << 12,
    kTaskInputMemorySize = 1 << 19,
    kTaskOutputMemorySize = 1 << 19,
    kMcsLockMemorySize = 1 << 19,
  };
  ThreadMemoryAnchors() { std::memset(this, 0, sizeof(*this)); }
  ~ThreadMemoryAnchors() {}

  // No copying
  ThreadMemoryAnchors(const ThreadMemoryAnchors &other) CXX11_FUNC_DELETE;
  ThreadMemoryAnchors& operator=(const ThreadMemoryAnchors &other) CXX11_FUNC_DELETE;

  /**
   * Status and synchronization mechanism for impersonation of this thread.
   * Always 4kb.
   */
  void*           impersonate_status_memory_;

  /**
   * Input buffer for an impersonated task.
   * Always 512kb (so far, make it configurable later).
   */
  void*           task_input_memory_;

  /**
   * Output buffer for an impersonated task.
   * Always 512kb (so far, make it configurable later).
   */
  void*           task_output_memory_;

  /**
    * Pre-allocated MCS block for each thread. Index is node-local thread ordinal.
    * Array of McsBlock.
    * 512kb (==sizeof(McsBlock) * 64k) for each thread.
    */
  xct::McsBlock*  mcs_lock_memories_;
};

/**
 * @brief Repository of all shared memory in one FOEDUS instance.
 * @ingroup SOC
 * @details
 * These are the memories shared across SOCs, which are allocated at initialization.
 * Be super careful on objects placed in these shared memories. You can't put objects with
 * heap-allocated contents, such as std::string.
 */
class SharedMemoryRepo CXX11_FINAL {
 public:
  SharedMemoryRepo() :
    soc_count_(0),
    my_soc_id_(0),
    node_memories_(CXX11_NULLPTR),
    node_memory_anchors_(CXX11_NULLPTR),
    volatile_pools_(CXX11_NULLPTR) {}
  ~SharedMemoryRepo() { deallocate_shared_memories(); }

  // Disable copy constructor
  SharedMemoryRepo(const SharedMemoryRepo &other) CXX11_FUNC_DELETE;
  SharedMemoryRepo& operator=(const SharedMemoryRepo &other) CXX11_FUNC_DELETE;

  /** Master process creates shared memories by calling this method*/
  ErrorStack  allocate_shared_memories(const EngineOptions& options);

  /**
   * Child processes (emulated or not) set a reference to shared memory and receive
   * the EngnieOption value by calling this method.
   * @param[in] master_upid Universal (or Unique) ID of the master process. This is the only
   * parameter that has to be passed from the master process to child processes.
   * @param[in] my_soc_id SOC ID of this node.
   * @param[out] options One of the shared memory contains the EngineOption values.
   * This method also retrieves them from the shared memory.
   */
  ErrorStack  attach_shared_memories(uint64_t master_upid, SocId my_soc_id, EngineOptions* options);

  /**
   * @brief Marks shared memories as being removed so that it will be reclaimed when all processes
   * detach it.
   * @details
   * This is part of deallocation of shared memory in master process.
   * After calling this method, no process can attach this shared memory. So, do not call this
   * too early. However, on the other hand, do not call this too late.
   * If the master process dies for an unexpected reason, the shared memory remains until
   * next reboot. Call it as soon as child processes ack-ed that they have attached the memory
   * or that there are some issues the master process should exit.
   * This method is idempotent, meaning you can safely call this many times.
   */
  void        mark_for_release();

  /** Detaches and releases the shared memories. In child processes, this just detaches. */
  void        deallocate_shared_memories();

  void*       get_global_memory() { return global_memory_.get_block(); }
  GlobalMemoryAnchors* get_global_memory_anchors() { return &global_memory_anchors_; }

  void        change_master_status(MasterEngineStatus::StatusCode new_status);
  MasterEngineStatus::StatusCode get_master_status() const;

  void*       get_node_memory(SocId node) { return node_memories_[node].get_block(); }
  NodeMemoryAnchors* get_node_memory_anchors(SocId node) { return &node_memory_anchors_[node]; }

  void        change_child_status(SocId node, ChildEngineStatus::StatusCode new_status);
  ChildEngineStatus::StatusCode get_child_status(SocId node) const;

  void*       get_volatile_pool(SocId node) { return volatile_pools_[node].get_block(); }

 private:
  SocId                 soc_count_;
  SocId                 my_soc_id_;

  /**
   * Each module needs a small shared memory. This packs all of them into one shared memory.
   * This memory is either tied to NUMA node-0 or intereleaved, so no locality.
   * Per-node memories are separated to their own shared memories below.
   */
  memory::SharedMemory  global_memory_;
  GlobalMemoryAnchors   global_memory_anchors_;

  /**
   * Per-node memories that have to be accessible to other SOCs. Index is SOC-id.
   *  \li Channel memory between root and individual SOCs.
   *  \li All inputs/output of impersonated sessions are stored in this memory.
   *  \li Memory for MCS-locking.
   *  \li etc
   * Each memory is tied to an SOC.
   */
  memory::SharedMemory* node_memories_;
  NodeMemoryAnchors*    node_memory_anchors_;

  /**
   * Memory for volatile page pool in each SOC. Index is SOC-id.
   * This is by far the biggest shared memory.
   * These are tied to each SOC, but still shared.
   */
  memory::SharedMemory* volatile_pools_;
  // this is just one contiguous memory. no anchors needed.

  void init_empty(const EngineOptions& options);
  void set_global_memory_anchors(uint64_t xml_size, const EngineOptions& options);
  void set_node_memory_anchors(SocId node, const EngineOptions& options);
  static uint64_t calculate_global_memory_size(uint64_t xml_size, const EngineOptions& options);
  static uint64_t calculate_node_memory_size(const EngineOptions& options);

  /** subroutine of allocate_shared_memories() launched on its own thread */
  static void allocate_one_node(
    uint16_t node,
    uint64_t node_memory_size,
    uint64_t volatile_pool_size,
    SharedMemoryRepo* repo);
};


}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_
