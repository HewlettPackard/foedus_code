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
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
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
  McsBlock* get_ww_my_block(McsBlockIndex index);
  /** Dereference my block index for reader-writer locks */
  RW_BLOCK* get_rw_my_block(McsBlockIndex index);

  /** Dereference other thread's block index for exclusive locks */
  McsBlock* get_ww_other_block(thread::ThreadId id, McsBlockIndex index);
  /** Dereference other thread's block index for reader-writer locks */
  RW_BLOCK* get_rw_other_block(thread::ThreadId id, McsBlockIndex index);
  /** Dereference other thread's block index for reader-writer locks, but receives a block int */
  RW_BLOCK* get_rw_other_block(uint32_t block_int);

  /** same as above, but receives a combined int in For McsRwLock */
  RW_BLOCK* dereference_rw_tail_block(uint32_t tail_int);

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
    mcs_block_current_ = rhs.mcs_block_current_;
    mcs_waiting_.store(rhs.mcs_waiting_.load());  // mainly due to this guy, this is NOT a move
  }
  void init(uint32_t max_block_count) {
    mcs_ww_blocks_.resize(max_block_count);
    mcs_rw_blocks_.resize(max_block_count);
    mcs_block_current_ = 0;
    mcs_waiting_ = false;
  }

  std::vector<McsBlock>   mcs_ww_blocks_;
  std::vector< RW_BLOCK > mcs_rw_blocks_;
  uint32_t                mcs_block_current_;
  std::atomic<bool>       mcs_waiting_;
  // add more if we need more context
};

/**
 * Analogous to one thread-group/socket/node.
 * @note completely header-only
 */
template<typename RW_BLOCK>
struct McsMockNode {
  void init(uint32_t threads_per_node, uint32_t max_block_count) {
    threads_.resize(threads_per_node);
    for (uint32_t t = 0; t < threads_per_node; ++t) {
      threads_[t].init(max_block_count);
    }
  }

  std::vector< McsMockThread<RW_BLOCK> >  threads_;
};

/**
 * Analogous to the entire engine.
 * @note completely header-only
 */
template<typename RW_BLOCK>
struct McsMockContext {
  void init(uint32_t nodes, uint32_t threads_per_node, uint32_t max_block_count) {
    nodes_.resize(nodes);
    for (uint32_t n = 0; n < nodes; ++n) {
      nodes_[n].init(threads_per_node, max_block_count);
    }
  }

  std::vector< McsMockNode<RW_BLOCK> >    nodes_;
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

  McsBlock* get_ww_my_block(McsBlockIndex index) {
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
  McsBlock* get_ww_other_block(thread::ThreadId id, McsBlockIndex index) {
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

 private:
  const thread::ThreadId            id_;
  const thread::ThreadGroupId       numa_node_;
  const thread::ThreadLocalOrdinal  local_ordinal_;
  McsMockContext<RW_BLOCK>* const   context_;
  McsMockThread<RW_BLOCK>* const    me_;
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MCS_ADAPTER_IMPL_HPP_
