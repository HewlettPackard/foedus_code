/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
#define FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_

#include <string>
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace memory {
/**
 * @brief Repository of memories dynamically acquired and shared within one NUMA node (socket).
 * @ingroup MEMHIERARCHY THREAD
 * @details
 * One NumaNodeMemory corresponds to one foedus::thread::ThreadGroup.
 * All threads in the thread group belong to the NUMA node, thus sharing memories between
 * them must be efficient.
 * So, all memories here are allocated/freed via ::numa_alloc_interleaved(), ::numa_alloc_onnode(),
 * and ::numa_free() (except the user specifies to not use them).
 */
class NumaNodeMemory CXX11_FINAL : public DefaultInitializable {
 public:
  NumaNodeMemory() CXX11_FUNC_DELETE;
  NumaNodeMemory(Engine* engine, foedus::thread::ThreadGroupId numa_node);
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  foedus::thread::ThreadGroupId   get_numa_node() const { return numa_node_; }

  PagePool&                       get_volatile_pool() { return volatile_pool_; }
  PagePool&                       get_snapshot_pool() { return snapshot_pool_; }
  cache::CacheHashtable*          get_snapshot_cache_table() { return snapshot_cache_table_; }

  // accessors for child memories
  foedus::thread::ThreadLocalOrdinal get_core_memory_count() const {
    ASSERT_ND(core_memories_.size() <= foedus::thread::kMaxThreadLocalOrdinal);
    return static_cast<foedus::thread::ThreadLocalOrdinal>(core_memories_.size());
  }
  std::vector<NumaCoreMemory*>& get_core_memories() { return core_memories_; }
  NumaCoreMemory* get_core_memory(foedus::thread::ThreadId id) const {
    return core_memories_[foedus::thread::decompose_numa_local_ordinal(id)];
  }
  NumaCoreMemory* get_core_memory(foedus::thread::ThreadLocalOrdinal ordinal) const {
    return core_memories_[ordinal];
  }

  /**
   * Allocate a memory of the given size on this NUMA node.
   * @param[in] size byte size of the memory to acquire
   * @param[in] alignment alignment size
   * @param[out] out allocated memory is moved to object
   * @return Expect OUTOFMEMORY error.
   */
  ErrorStack      allocate_numa_memory_general(
    uint64_t size,
    uint64_t alignment,
    AlignedMemory *out) const;
  ErrorStack      allocate_numa_memory(uint64_t size, AlignedMemory *out) const {
    return allocate_numa_memory_general(size, 1 << 12, out);
  }
  ErrorStack      allocate_huge_numa_memory(uint64_t size, AlignedMemory *out) const {
    return allocate_numa_memory_general(size, kHugepageSize, out);
  }

  AlignedMemory& get_read_set_memory() { return read_set_memory_; }
  xct::XctAccess* get_read_set_memory_piece(thread::ThreadLocalOrdinal core_ordinal) {
    return read_set_memory_pieces_[core_ordinal];
  }
  AlignedMemory& get_write_set_memory() { return write_set_memory_; }
  xct::WriteXctAccess* get_write_set_memory_piece(thread::ThreadLocalOrdinal core_ordinal) {
    return write_set_memory_pieces_[core_ordinal];
  }
  AlignedMemory& get_lock_free_write_set_memory() { return lock_free_write_set_memory_; }
  xct::LockFreeWriteXctAccess* get_lock_free_write_set_memory_piece(
    thread::ThreadLocalOrdinal core_ordinal) {
    return lock_free_write_set_memory_pieces_[core_ordinal];
  }
  PagePoolOffsetChunk* get_volatile_offset_chunk_memory_piece(
    foedus::thread::ThreadLocalOrdinal core_ordinal) {
    return volatile_offset_chunk_memory_pieces_[core_ordinal];
  }
  PagePoolOffsetChunk* get_snapshot_offset_chunk_memory_piece(
    foedus::thread::ThreadLocalOrdinal core_ordinal) {
    return snapshot_offset_chunk_memory_pieces_[core_ordinal];
  }
  AlignedMemorySlice get_log_buffer_memory_piece(log::LoggerId logger) {
    return log_buffer_memory_pieces_[logger];
  }

  /** Report rough statistics of free memory */
  std::string         dump_free_memory_stat() const;

 private:
  /** initialize read-set and write-set memory. */
  ErrorStack      initialize_read_write_set_memory();
  /** initialize page_offset_chunk_memory_/page_offset_chunk_memory_pieces_. */
  ErrorStack      initialize_page_offset_chunk_memory();
  /** initialize log_buffer_memory_. */
  ErrorStack      initialize_log_buffers_memory();
  /** initialize child memories per core */
  ErrorStack      initialize_core_memory(thread::ThreadLocalOrdinal ordinal);

  Engine* const                           engine_;

  /**
   * The NUMA node this memory is allocated for.
   */
  const foedus::thread::ThreadGroupId     numa_node_;

  /** Number of cores in this node. */
  const thread::ThreadLocalOrdinal        cores_;

  /** Number of loggers in this node. */
  const uint16_t                          loggers_;

  /** In-memory volatile page pool in this node. */
  PagePool                                volatile_pool_;
  /** In-memory snapshot page pool in this node. */
  PagePool                                snapshot_pool_;
  /** Memory for hash buckets for snapshot_cache_table_. */
  AlignedMemory                           snapshot_hashtable_memory_;
  /** Hashtable for in-memory snapshot page pool in this node. */
  cache::CacheHashtable*                  snapshot_cache_table_;

  /**
   * List of NumaCoreMemory, one for each core in this node.
   * Index is local ordinal of the NUMA cores.
   */
  std::vector<NumaCoreMemory*>            core_memories_;

  /**
   * Memory to keep track of read-set during transactions.
   * To better utilize HugePages, we allocate this in node level for all cores rather than in
   * individual core level. NumaCoreMemory merely gets a piece of this memory.
   */
  AlignedMemory                           read_set_memory_;
  std::vector<xct::XctAccess*>            read_set_memory_pieces_;

  /**
   * Memory to keep track of write-set during transactions. Same above.
   */
  AlignedMemory                           write_set_memory_;
  std::vector<xct::WriteXctAccess*>       write_set_memory_pieces_;

  /**
   * Memory to keep track of lock-free write-set during transactions. Same above.
   */
  AlignedMemory                             lock_free_write_set_memory_;
  std::vector<xct::LockFreeWriteXctAccess*> lock_free_write_set_memory_pieces_;

  /**
   * Memory to hold a \b local pool of pointers to free volatile pages. Same above.
   */
  AlignedMemory                           volatile_offset_chunk_memory_;
  std::vector<PagePoolOffsetChunk*>       volatile_offset_chunk_memory_pieces_;

  /**
   * Memory to hold a \b local pool of pointers to free snapshot pages. Same above.
   */
  AlignedMemory                           snapshot_offset_chunk_memory_;
  std::vector<PagePoolOffsetChunk*>       snapshot_offset_chunk_memory_pieces_;

  /**
   * Memory to hold a thread's log buffer. Split by each core in this node.
   */
  AlignedMemory                           log_buffer_memory_;
  std::vector<AlignedMemorySlice>         log_buffer_memory_pieces_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
