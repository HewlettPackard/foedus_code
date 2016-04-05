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
#ifndef FOEDUS_XCT_XCT_MCS_ADAPTER_IMPL_HPP_
#define FOEDUS_XCT_XCT_MCS_ADAPTER_IMPL_HPP_

#include <atomic>
#include <memory>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief Defines an adapter template interface for our MCS lock classes.
 * @ingroup XCT
 * @details
 * This object is \b NOT used by itself at all.
 * Rather, this just defines the interface a template object must meet for our MCS lock classes.
 * In other words, this is something like template-concept. Hey, but it's yet to come in standard..
 *
 * No virtual method, no inheritance. This is a \e template interface that inlines everything.
 * We can't afford virtual function calls for every single MCS lock.
 * In release compilation, all traces of these objects should disappear.
 *
 * @par ww/rw
 * In some methods, we have prefix \e ww (write-write or exclusive) or \e rw (reader-writer).
 * This is because we currently keep both the original MCSg lock and a reader-writer version
 * in the code. Once we integrate everything to rw, we might eliminate the ww versions.
 *
 * @tparam RW_BLOCK Queue node object for RW-lock. Either McsRwSimpleBlock or McsRwExtendedBlock.
 */
template<typename RW_BLOCK>
class McsAdaptorConcept {
 public:
  /**
   * Issues a new queue node of this thread and returns its block index.
   * This typically maintains a counter in the concrete object.
   */
  McsBlockIndex issue_new_block();
  McsBlockIndex get_cur_block() const;
  McsBlockIndex get_other_cur_block(thread::ThreadId id);

  /** Returns thread-Id of this thread */
  thread::ThreadId      get_my_id() const;
  /** Returns group-Id of this thread */
  thread::ThreadGroupId get_my_numa_node() const;

  /** Returns the atomic bool var on whether current thread is waiting for some lock */
  std::atomic<bool>* me_waiting();

  /** Returns the bool var on whether other thread is waiting for some lock */
  std::atomic<bool>* other_waiting(thread::ThreadId id);

  /** Dereference my block index for exclusive locks */
  McsWwBlock* get_ww_my_block(McsBlockIndex index);
  /** Dereference my block index for reader-writer locks */
  RW_BLOCK* get_rw_my_block(McsBlockIndex index);

  /** Dereference other thread's block index for exclusive locks */
  McsWwBlock* get_ww_other_block(thread::ThreadId id, McsBlockIndex index);
  /** Dereference other thread's block index for reader-writer locks */
  RW_BLOCK* get_rw_other_block(thread::ThreadId id, McsBlockIndex index);
  /** Dereference other thread's block index for reader-writer locks, but receives a block int */
  RW_BLOCK* get_rw_other_block(uint32_t block_int);

  /** same as above, but receives a combined int in For McsRwLock */
  RW_BLOCK* dereference_rw_tail_block(uint32_t tail_int);

  /** Dereference other thread's block index for extended rwlock. This will go over
   * the TLS lock-block_index mapping to find out the block index. For writers only. */
  McsRwExtendedBlock* get_rw_other_async_block(thread::ThreadId id, xct::McsRwLock* lock);

  void add_rw_async_mapping(xct::McsRwLock* lock, xct::McsBlockIndex block_index);

 private:
  McsAdaptorConcept() = delete;
  ~McsAdaptorConcept() = delete;
};

/**
 * A dummy implementation that provides McsAdaptorConcept for testing.
 * A testcase shares an instance of this object and creates  below to
 * test locking logics easily without any other FOEDUS engine pieces.
 * These \e mock objects are only for testing, thus no performance optimization at all.
 *
 * You might feel it weird to have classes for testing here,
 * but we need to define them here to allow explicit instantiation of templated
 * functions receiving these objects within xct_mcs_impl.cpp.
 * Otherwise we have to define, not just declare, the templated funcs in hpp.
 * @note completely header-only
 */
template<typename RW_BLOCK>
struct McsMockThread {
  McsMockThread() = default;
  McsMockThread(McsMockThread&& rhs) {
    // heh, std::vector::resize() demands a move constructor.
    mcs_ww_blocks_ = std::move(rhs.mcs_ww_blocks_);
    mcs_rw_blocks_ = std::move(rhs.mcs_rw_blocks_);
    mcs_rw_async_mappings_ = std::move(rhs.mcs_rw_async_mappings_);
    mcs_block_current_ = rhs.mcs_block_current_;
    mcs_waiting_.store(rhs.mcs_waiting_.load());  // mainly due to this guy, this is NOT a move
  }
  void init(uint32_t max_block_count) {
    mcs_ww_blocks_.resize(max_block_count);
    mcs_rw_blocks_.resize(max_block_count);
    mcs_rw_async_mappings_.resize(max_block_count);
    mcs_rw_async_mapping_current_ = 0;
    mcs_block_current_ = 0;
    mcs_waiting_ = false;
  }
  xct::McsBlockIndex get_mcs_rw_async_block_index(
    const memory::GlobalVolatilePageResolver& resolver,
    xct::McsRwLock* lock) {
    auto ulockid = xct::rw_lock_to_universal_lock_id(resolver, lock);
    for (auto &m : mcs_rw_async_mappings_) {
      if (m.lock_id_ == ulockid) {
        return m.block_index_;
      }
    }
    return 0;
  }

  std::vector<McsWwBlock>   mcs_ww_blocks_;
  std::vector< RW_BLOCK > mcs_rw_blocks_;
  std::vector<xct::McsRwAsyncMapping> mcs_rw_async_mappings_;
  uint32_t                mcs_rw_async_mapping_current_;
  uint32_t                mcs_block_current_;
  std::atomic<bool>       mcs_waiting_;
  // add more if we need more context
};

constexpr uint32_t kMcsMockDataPageHeaderSize = 128U;
constexpr uint32_t kMcsMockDataPageHeaderPad
  = kMcsMockDataPageHeaderSize - sizeof(storage::PageHeader);
constexpr uint32_t kMcsMockDataPageLocksPerPage
  = (storage::kPageSize - kMcsMockDataPageHeaderSize)
  / (sizeof(RwLockableXctId) + sizeof(McsWwLock));
constexpr uint32_t kMcsMockDataPageFiller
  = (storage::kPageSize - kMcsMockDataPageHeaderSize)
    - (sizeof(RwLockableXctId) + sizeof(McsWwLock)) * kMcsMockDataPageLocksPerPage;


/**
 * A dummy page layout to store RwLockableXctId.
 * We need to fake a valid page layout because that's what our UniversalLockId conversion logic
 * assumes.
 */
struct McsMockDataPage {
  void init(storage::StorageId dummy_storage_id, uint16_t node_id, uint32_t in_node_index) {
    storage::VolatilePagePointer page_id;
    page_id.set(node_id, in_node_index);
    header_.init_volatile(page_id, dummy_storage_id, storage::kArrayPageType);
    for (uint32_t i = 0; i < kMcsMockDataPageLocksPerPage; ++i) {
      tid_[i].reset();
      ww_[i].reset();
    }
  }
  storage::PageHeader header_;      // +40 -> 40
  char                header_pad_[kMcsMockDataPageHeaderPad];  // -> 128
  RwLockableXctId     tid_[kMcsMockDataPageLocksPerPage];
  McsWwLock             ww_[kMcsMockDataPageLocksPerPage];
  char                filler_[kMcsMockDataPageFiller];
};

/**
 * Analogous to one thread-group/socket/node.
 * @note completely header-only
 */
template<typename RW_BLOCK>
struct McsMockNode {
  void init(
    storage::StorageId dummy_storage_id,
    uint16_t node_id,
    uint32_t threads_per_node,
    uint32_t max_block_count,
    uint32_t pages_per_node) {
    node_id_ = node_id;
    max_block_count_ = max_block_count;
    threads_.resize(threads_per_node);
    for (uint32_t t = 0; t < threads_per_node; ++t) {
      threads_[t].init(max_block_count);
    }

    page_memory_.alloc_onnode(
      sizeof(McsMockDataPage) * pages_per_node,
      sizeof(McsMockDataPage),
      node_id);
    ASSERT_ND(!page_memory_.is_null());
    pages_ = reinterpret_cast<McsMockDataPage*>(page_memory_.get_block());
    for (uint32_t i = 0; i < pages_per_node; ++i) {
      pages_[i].init(dummy_storage_id, node_id, i);
    }
  }

  uint16_t node_id_;
  uint32_t max_block_count_;
  std::vector< McsMockThread<RW_BLOCK> >  threads_;

  /**
   * Locks assigned to this node are stored in these memory.
   */
  McsMockDataPage*      pages_;
  memory::AlignedMemory page_memory_;
};

/**
 * Analogous to the entire engine.
 * @note completely header-only
 */
template<typename RW_BLOCK>
struct McsMockContext {
  void init(
    storage::StorageId dummy_storage_id,
    uint32_t nodes,
    uint32_t threads_per_node,
    uint32_t max_block_count,
    uint32_t max_lock_count) {
    max_block_count_ = max_block_count;
    max_lock_count_ = max_lock_count;
    // + 1U for index-0 (which is not used), and +1U for ceiling
    pages_per_node_ = (max_lock_count_ / kMcsMockDataPageLocksPerPage) + 1U + 1U;
    nodes_.resize(nodes);
    page_memory_resolver_.numa_node_count_ = nodes;
    page_memory_resolver_.begin_ = 1U;
    page_memory_resolver_.end_ = pages_per_node_;
    ASSERT_ND(page_memory_resolver_.begin_ < page_memory_resolver_.end_);
    for (uint32_t n = 0; n < nodes; ++n) {
      nodes_[n].init(dummy_storage_id, n, threads_per_node, max_block_count, pages_per_node_);
      page_memory_resolver_.bases_[n] = reinterpret_cast<storage::Page*>(nodes_[n].pages_);
    }
  }

  RwLockableXctId* get_rw_lock_address(uint16_t node_id, uint64_t lock_index) {
    ASSERT_ND(lock_index < max_lock_count_);
    ASSERT_ND(node_id < nodes_.size());
    const uint64_t page_index = lock_index / kMcsMockDataPageLocksPerPage + 1U;
    const uint64_t lock_in_page_index = lock_index % kMcsMockDataPageLocksPerPage;;
    ASSERT_ND(page_index < pages_per_node_);
    McsMockDataPage* page = nodes_[node_id].pages_ + page_index;
    return page->tid_ + lock_in_page_index;
  }
  McsWwLock*        get_ww_lock_address(uint16_t node_id, uint64_t lock_index) {
    ASSERT_ND(lock_index < max_lock_count_);
    ASSERT_ND(node_id < nodes_.size());
    const uint64_t page_index = lock_index / kMcsMockDataPageLocksPerPage + 1U;
    const uint64_t lock_in_page_index = lock_index % kMcsMockDataPageLocksPerPage;;
    ASSERT_ND(page_index < pages_per_node_);
    McsMockDataPage* page = nodes_[node_id].pages_ + page_index;
    return page->ww_ + lock_in_page_index;
  }

  uint32_t max_block_count_;
  uint32_t max_lock_count_;
  uint32_t pages_per_node_;
  std::vector< McsMockNode<RW_BLOCK> >    nodes_;
  /**
   * All locks managed by this objects are placed in these memory regions.
   * Unlike the real engine, these are not shared-memory, but the page
   * resolver logic doesn't care whether it's shared-memory or not, so it's fine.
   */
  memory::GlobalVolatilePageResolver      page_memory_resolver_;
};

/**
 * Implements McsAdaptorConcept.
 * @note completely header-only
 */
template<typename RW_BLOCK>
class McsMockAdaptor {
 public:
  McsMockAdaptor(thread::ThreadId id, McsMockContext<RW_BLOCK>* context)
    : id_(id),
      numa_node_(thread::decompose_numa_node(id)),
      local_ordinal_(thread::decompose_numa_local_ordinal(id)),
      context_(context),
      me_(context->nodes_[numa_node_].threads_.data() + local_ordinal_) {}
  ~McsMockAdaptor() {}

  McsBlockIndex issue_new_block() { return ++me_->mcs_block_current_; }
  McsBlockIndex get_cur_block() const { return me_->mcs_block_current_; }
  thread::ThreadId      get_my_id() const { return id_; }
  thread::ThreadGroupId get_my_numa_node() const { return numa_node_; }
  std::atomic<bool>* me_waiting() { return &me_->mcs_waiting_; }

  McsWwBlock* get_ww_my_block(McsBlockIndex index) {
    ASSERT_ND(index <= me_->mcs_block_current_);
    return me_->mcs_ww_blocks_.data() + index;
  }
  RW_BLOCK* get_rw_my_block(McsBlockIndex index) {
    ASSERT_ND(index <= me_->mcs_block_current_);
    return me_->mcs_rw_blocks_.data() + index;
  }

  McsMockThread<RW_BLOCK>* get_other_thread(thread::ThreadId id) {
    thread::ThreadGroupId node = thread::decompose_numa_node(id);
    ASSERT_ND(node < context_->nodes_.size());
    thread::ThreadLocalOrdinal ordinal = thread::decompose_numa_local_ordinal(id);
    ASSERT_ND(ordinal < context_->nodes_[node].threads_.size());
    return context_->nodes_[node].threads_.data() + ordinal;
  }
  std::atomic<bool>* other_waiting(thread::ThreadId id) {
    McsMockThread<RW_BLOCK>* other = get_other_thread(id);
    return &(other->mcs_waiting_);
  }
  McsBlockIndex get_other_cur_block(thread::ThreadId id) {
    McsMockThread<RW_BLOCK>* other = get_other_thread(id);
    return other->mcs_block_current_;
  }
  McsWwBlock* get_ww_other_block(thread::ThreadId id, McsBlockIndex index) {
    McsMockThread<RW_BLOCK>* other = get_other_thread(id);
    ASSERT_ND(index <= other->mcs_block_current_);
    return other->mcs_ww_blocks_.data() + index;
  }
  RW_BLOCK* get_rw_other_block(thread::ThreadId id, McsBlockIndex index) {
    McsMockThread<RW_BLOCK>* other = get_other_thread(id);
    ASSERT_ND(index <= other->mcs_block_current_);
    return other->mcs_rw_blocks_.data() + index;
  }
  RW_BLOCK* get_rw_other_block(uint32_t block_int) {
    McsMockThread<RW_BLOCK>* other = get_other_thread(block_int >> 16);
    McsBlockIndex index = block_int & 0xFFFFU;
    ASSERT_ND(index <= other->mcs_block_current_);
    return other->mcs_rw_blocks_.data() + index;
  }
  RW_BLOCK* dereference_rw_tail_block(uint32_t tail_int) {
    McsRwLock tail_tmp;
    tail_tmp.tail_ = tail_int;
    uint32_t tail_id = tail_tmp.get_tail_waiter();
    uint32_t tail_block = tail_tmp.get_tail_waiter_block();
    return get_rw_other_block(tail_id, tail_block);
  }
  McsRwExtendedBlock* get_rw_other_async_block(thread::ThreadId id, xct::McsRwLock* lock) {
    McsMockThread<RW_BLOCK>* other = get_other_thread(id);
    McsBlockIndex block
      = other->get_mcs_rw_async_block_index(context_->page_memory_resolver_, lock);
    ASSERT_ND(block);
    return get_rw_other_block(id, block);
  }
  void add_rw_async_mapping(xct::McsRwLock* lock, xct::McsBlockIndex block_index) {
    ASSERT_ND(lock);
    ASSERT_ND(block_index);
    auto ulockid = xct::rw_lock_to_universal_lock_id(context_->page_memory_resolver_, lock);
#ifndef NDEBUG
    for (uint32_t i = 0; i < me_->mcs_rw_async_mapping_current_; ++i) {
      if (me_->mcs_rw_async_mappings_[i].lock_id_ == ulockid) {
        ASSERT_ND(false);
      }
    }
#endif
    // TLS, no concurrency control needed
    auto index = me_->mcs_rw_async_mapping_current_++;
    ASSERT_ND(me_->mcs_rw_async_mappings_[index].lock_id_ == kNullUniversalLockId);
    me_->mcs_rw_async_mappings_[index].lock_id_ = ulockid;
    me_->mcs_rw_async_mappings_[index].block_index_ = block_index;
  }
  void remove_rw_async_mapping(xct::McsRwLock* lock) {
    ASSERT_ND(me_->mcs_rw_async_mapping_current_);
    auto lock_id = xct::rw_lock_to_universal_lock_id(context_->page_memory_resolver_, lock);
    for (uint32_t i = 0; i < me_->mcs_rw_async_mapping_current_; ++i) {
      if (me_->mcs_rw_async_mappings_[i].lock_id_ == lock_id) {
        me_->mcs_rw_async_mappings_[i].lock_id_ = kNullUniversalLockId;
        --me_->mcs_rw_async_mapping_current_;
        return;
      }
    }
    ASSERT_ND(false);
  }

 private:
  const thread::ThreadId            id_;
  const thread::ThreadGroupId       numa_node_;
  const thread::ThreadLocalOrdinal  local_ordinal_;
  McsMockContext<RW_BLOCK>* const   context_;
  McsMockThread<RW_BLOCK>* const    me_;
};

static_assert(sizeof(McsMockDataPage) == storage::kPageSize, "McsMockDataPage not in kPageSize?");

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MCS_ADAPTER_IMPL_HPP_
