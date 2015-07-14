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
#ifndef FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_
#define FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_

#include <stdint.h>

#include <cstring>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/module_type.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/protected_boundary.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/shared_memory.hpp"
#include "foedus/proc/fwd.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/restart/fwd.hpp"
#include "foedus/savepoint/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/soc/fwd.hpp"
#include "foedus/soc/soc_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"
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
     * Child SOCs resume their initialization after observing this status.
     */
    kSharedMemoryReservedReclamation,

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
    ASSERT_ND(new_status == kFatalError || new_status == kWaitingForChildTerminate ||
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

  void change_init_atomic(ModuleType value) {
    ASSERT_ND(static_cast<int>(initialized_modules_) == static_cast<int>(value) - 1);
    assorted::memory_fence_acq_rel();
    initialized_modules_ = value;
    assorted::memory_fence_acq_rel();
  }
  void change_uninit_atomic(ModuleType value) {
    ASSERT_ND(static_cast<int>(uninitialized_modules_) == static_cast<int>(value) + 1);
    assorted::memory_fence_acq_rel();
    uninitialized_modules_ = value;
    assorted::memory_fence_acq_rel();
  }

  StatusCode status_code_;
  /** The module that has been most recently initialized in master. Used to synchronize init. */
  ModuleType initialized_modules_;
  /** The module that has been most recently closed in master. Used to synchronize uninit. */
  ModuleType uninitialized_modules_;
  // so far this is the only information.
};

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
     * kWaitingForChildInitialization status.
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

  void change_init_atomic(ModuleType value) {
    ASSERT_ND(static_cast<int>(initialized_modules_) == static_cast<int>(value) - 1);
    assorted::memory_fence_acq_rel();
    initialized_modules_ = value;
    assorted::memory_fence_acq_rel();
  }
  void change_uninit_atomic(ModuleType value) {
    ASSERT_ND(static_cast<int>(uninitialized_modules_) == static_cast<int>(value) + 1);
    assorted::memory_fence_acq_rel();
    uninitialized_modules_ = value;
    assorted::memory_fence_acq_rel();
  }

  StatusCode status_code_;
  /** The module that has been most recently initialized in this node. Used to synchronize init. */
  ModuleType initialized_modules_;
  /** The module that has been most recently closed in this node. Used to synchronize uninit. */
  ModuleType uninitialized_modules_;
  // so far this is the only information.
};

/**
 * Just a set of pointers within global_memory_ for ease of use.
 * All pointers are at least 8-byte aligned. Most of them are 4kb aligned.
 * @ingroup SOC
 */
struct GlobalMemoryAnchors {
  enum Constants {
    kMasterStatusMemorySize = 1 << 12,
    kLogManagerMemorySize = 1 << 12,
    kMetaLoggerSize = 1 << 13,
    kRestartManagerMemorySize = 1 << 12,
    /** 3 << 19 is for FixedSavepoint. It's about 1.5MB */
    kSavepointManagerMemorySize = (3 << 19) + (1 << 12),
    kSnapshotManagerMemorySize = 1 << 12,
    kStorageManagerMemorySize = 1 << 12,
    kXctManagerMemorySize = 1 << 12,
    kStorageMemorySize = 1 << 12,
    kMaxBoundaries = 1 << 7,
  };

  GlobalMemoryAnchors() { clear(); }
  ~GlobalMemoryAnchors() {}
  void clear() { std::memset(this, 0, sizeof(*this)); }

  // No copying
  GlobalMemoryAnchors(const GlobalMemoryAnchors &other) CXX11_FUNC_DELETE;
  GlobalMemoryAnchors& operator=(const GlobalMemoryAnchors &other) CXX11_FUNC_DELETE;

  /** The beginning of global memory is an XML-serialized EngineOption. The byte size. */
  uint64_t        options_xml_length_;
  /** The xml itself. not null terminated. */
  char*           options_xml_;

  /**
   * This tiny piece of memory contains the current status of the master engine and
   * its synchronization mechanisms. Always 4kb.
   */
  MasterEngineStatus* master_status_memory_;

  /** Tiny memory for log manager. Always 4kb. */
  log::LogManagerControlBlock*              log_manager_memory_;
  /** Tiny memory for metadata logger. Always 4kb. */
  log::MetaLogControlBlock*                 meta_logger_memory_;
  /** Tiny memory for restart manager. Always 4kb. */
  restart::RestartManagerControlBlock*      restart_manager_memory_;
  /** Tiny memory for savepoint manager. Always 4kb. */
  savepoint::SavepointManagerControlBlock*  savepoint_manager_memory_;
  /** Tiny memory for snapshot manager. Always 4kb. */
  snapshot::SnapshotManagerControlBlock*    snapshot_manager_memory_;
  /** Tiny memory for storage manager. Always 4kb. */
  storage::StorageManagerControlBlock*      storage_manager_memory_;
  /** Tiny memory for xct manager. Always 4kb. */
  xct::XctManagerControlBlock*              xct_manager_memory_;

  /**
   * Tiny metadata memory for partitioners. It points to blocks in the following data memory.
   * The size is sizeof(storage::PartitionerMetadata) *  StorageOptions::max_storages_.
   */
  storage::PartitionerMetadata*             partitioner_metadata_;
  /**
   * Data block to place detailed information of partitioners.
   * The size is StorageOptions::partitioner_data_memory_mb_.
   */
  void*                                     partitioner_data_;

  /**
   * This memory stores the ID of storages sorted by their names.
   * The size is 4 (=sizeof(StorageId)) * StorageOptions::max_storages_.
   */
  storage::StorageId*                       storage_name_sort_memory_;

  /**
   * Status of each storage instance is stored in this shared memory.
   * The size for one storage must be within 4kb. If the storage type requires more than 4kb,
   * just grab a page from volatile page pools and point to it from the storage object.
   * sizeof(storage::StorageControlBlock) is exactly 4kb, so we can treat this like an array,
   * not an array of pointers.
   * The size is 4kb * StorageOptions::max_storages_.
   */
  storage::StorageControlBlock*             storage_memories_;

  /**
   * This 'user memory' can be
   * used for arbitrary purporses by the user to communicate between SOCs.
   * The size is SocOptions::shared_user_memory_size_kb_.
   */
  void*                                     user_memory_;

  /** sanity check boundaries to detect bogus memory accesses that overrun a memory region */
  assorted::ProtectedBoundary*              protected_boundaries_[kMaxBoundaries];
  /** To be a POD, we avoid vector and instead uses a fix-sized array */
  uint32_t                                  protected_boundaries_count_;
  /** whether we have invoked mprotect on them */
  bool                                      protected_boundaries_needs_release_;
};

/**
 * Same as GlobalMemoryAnchors except this is for node_memories_.
 * @ingroup SOC
 */
struct NodeMemoryAnchors {
  enum Constants {
    kChildStatusMemorySize = 1 << 12,
    kPagePoolMemorySize = 1 << 12,
    kLogReducerMemorySize = 1 << 12,
    kLoggerMemorySize = 1 << 21,
    kProcManagerMemorySize = 1 << 12,
    kMaxBoundaries = 1 << 12,
  };

  NodeMemoryAnchors() { clear(); }
  ~NodeMemoryAnchors() { deallocate_arrays(); }
  void clear() { std::memset(this, 0, sizeof(*this)); }

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
  memory::PagePoolControlBlock*   volatile_pool_status_;

  /**
   * ProcManagers's status and its synchronization mechanism on this node.
   * Always 4kb.
   */
  proc::ProcManagerControlBlock*  proc_manager_memory_;
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
   * Tiny control memory for LogReducer in this node.
   * Always 4kb.
   * The reducer buffers are allocated separately below.
   */
  snapshot::LogReducerControlBlock* log_reducer_memory_;

  /**
   * Actual buffers for LogReducer. Two for switching.
   * Size is SnapshotOptions::log_reducer_buffer_mb_ in total of the two.
   */
  void*               log_reducer_buffers_[2];

  /**
   * This is the 'output' of the reducer in this node.
   * Each page contains a root-info page of one storage processed in the reducer.
   * Size is StorageOptions::max_storages_ * 4kb.
   */
  storage::Page*      log_reducer_root_info_pages_;

  /**
   * Status and synchronization mechanism for loggers on this node.
   * Index is node-local logger ID.
   * 4kb for each logger.
   */
  log::LoggerControlBlock** logger_memories_;

  /**
   * Anchors for each thread. Index is node-local thread ordinal.
   */
  ThreadMemoryAnchors*  thread_anchors_;

  /** sanity check boundaries to detect bogus memory accesses that overrun a memory region */
  assorted::ProtectedBoundary*              protected_boundaries_[kMaxBoundaries];
  /** To be a POD, we avoid vector and instead uses a fix-sized array */
  uint32_t                                  protected_boundaries_count_;
  /** whether we have invoked mprotect on them */
  bool                                      protected_boundaries_needs_release_;
};

/**
 * Part of NodeMemoryAnchors for each thread.
 * @ingroup SOC
 */
struct ThreadMemoryAnchors {
  enum Constants {
    kThreadMemorySize = 1 << 15,
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
   * Always 32kb (a bit large mainly due to the size of FixedErrorStack).
   */
  thread::ThreadControlBlock* thread_memory_;

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
  ErrorStack  allocate_shared_memories(uint64_t upid, Eid eid, const EngineOptions& options);

  /**
   * Child processes (emulated or not) set a reference to shared memory and receive
   * the EngnieOption value by calling this method.
   * @param[in] master_upid Universal (or Unique) ID of the master process. This is the
   * parameter that has to be passed from the master process to child processes.
   * @param[in] master_eid Engine ID of the master process. This is another
   * parameter that has to be passed from the master process to child processes.
   * @param[in] my_soc_id SOC ID of this node.
   * @param[out] options One of the shared memory contains the EngineOption values.
   * This method also retrieves them from the shared memory.
   */
  ErrorStack  attach_shared_memories(
    uint64_t master_upid,
    Eid master_eid,
    SocId my_soc_id,
    EngineOptions* options);

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
  void*       get_global_user_memory() { return global_memory_anchors_.user_memory_; }

  void        change_master_status(MasterEngineStatus::StatusCode new_status);
  MasterEngineStatus::StatusCode get_master_status() const;

  void*       get_node_memory(SocId node) { return node_memories_[node].get_block(); }
  NodeMemoryAnchors* get_node_memory_anchors(SocId node) { return &node_memory_anchors_[node]; }

  void        change_child_status(SocId node, ChildEngineStatus::StatusCode new_status);
  ChildEngineStatus::StatusCode get_child_status(SocId node) const;

  ThreadMemoryAnchors* get_thread_memory_anchors(thread::ThreadId thread_id) {
    SocId node = thread::decompose_numa_node(thread_id);
    uint16_t thread_ordinal = thread::decompose_numa_local_ordinal(thread_id);
    return &node_memory_anchors_[node].thread_anchors_[thread_ordinal];
  }

  void*       get_volatile_pool(SocId node) { return volatile_pools_[node].get_block(); }

  static uint64_t calculate_global_memory_size(uint64_t xml_size, const EngineOptions& options);
  static uint64_t calculate_node_memory_size(const EngineOptions& options);

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

  void set_global_memory_anchors(
    uint64_t xml_size,
    const EngineOptions& options,
    bool reset_boundaries);
  void put_global_memory_boundary(
    uint64_t* position,
    const std::string& name,
    bool reset_boundaries) {
    char* base = global_memory_.get_block();
    assorted::ProtectedBoundary* boundary
      = reinterpret_cast<assorted::ProtectedBoundary*>(base + (*position));
    if (reset_boundaries) {
      boundary->reset(name);
    }
    uint32_t next_index = global_memory_anchors_.protected_boundaries_count_;
    ASSERT_ND(next_index < GlobalMemoryAnchors::kMaxBoundaries);
    global_memory_anchors_.protected_boundaries_[next_index] = boundary;
    ++global_memory_anchors_.protected_boundaries_count_;
    *position += sizeof(assorted::ProtectedBoundary);
  }

  void set_node_memory_anchors(SocId node, const EngineOptions& options, bool reset_boundaries);
  void put_node_memory_boundary(
    SocId node,
    uint64_t* position,
    const std::string& name,
    bool reset_boundaries) {
    char* base = node_memories_[node].get_block();
    assorted::ProtectedBoundary* boundary
      = reinterpret_cast<assorted::ProtectedBoundary*>(base + (*position));
    if (reset_boundaries) {
      boundary->reset(name);
    }
    uint32_t next_index = node_memory_anchors_[node].protected_boundaries_count_;
    ASSERT_ND(next_index < NodeMemoryAnchors::kMaxBoundaries);
    node_memory_anchors_[node].protected_boundaries_[next_index] = boundary;
    ++node_memory_anchors_[node].protected_boundaries_count_;
    *position += sizeof(assorted::ProtectedBoundary);
  }


  /** subroutine of allocate_shared_memories() launched on its own thread */
  static void allocate_one_node(
    uint64_t upid,
    Eid eid,
    uint16_t node,
    uint64_t node_memory_size,
    uint64_t volatile_pool_size,
    ErrorStack* alloc_result,
    SharedMemoryRepo* repo);
};

CXX11_STATIC_ASSERT(
  sizeof(MasterEngineStatus) <= (1 << 12),
  "Size of MasterEngineStatus exceeds 4kb");

CXX11_STATIC_ASSERT(
  sizeof(ChildEngineStatus) <= (1 << 12),
  "Size of ChildEngineStatus exceeds 4kb");

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_
