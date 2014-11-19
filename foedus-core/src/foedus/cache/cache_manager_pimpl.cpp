/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/cache/cache_manager_pimpl.hpp"

#include <glog/logging.h>

#include <chrono>
#include <sstream>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace cache {

CacheManagerPimpl::CacheManagerPimpl(Engine* engine)
  : engine_(engine),
  stop_requested_(false),
  pool_(nullptr),
  hashtable_(nullptr),
  reclaimed_pages_(nullptr),
  reclaimed_pages_count_(0) {
}

ErrorStack CacheManagerPimpl::initialize_once() {
  if (engine_->is_master()) {
    // nothing to do in master engine
    return kRetOk;
  } else if (!engine_->get_memory_manager()->is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }

  LOG(INFO) << "Initializing Snapshot Cache in Node-" << engine_->get_soc_id() << "...";

  memory::NumaNodeMemory* node = engine_->get_memory_manager()->get_local_memory();
  pool_ = node->get_snapshot_pool();
  hashtable_ = node->get_snapshot_cache_table();
  reclaimed_pages_count_ = 0;

  // So far, the reclaimed_pages_memory_ is large enough to hold all pages in the pool at once.
  // This is obviously too much for most cases, but wouldn't be an issue as the memory consumption
  // is anyway negligibly smaller than the page pool itself. Keep it simple stupid.
  total_pages_ = pool_->get_stat().total_pages_;
  const CacheOptions& options = engine_->get_options().cache_;
  cleaner_threshold_ = total_pages_ * options.snapshot_cache_eviction_threshold_;
  urgent_threshold_ = total_pages_ * options.snapshot_cache_urgent_threshold_;
  ASSERT_ND(cleaner_threshold_ > 0);
  ASSERT_ND(urgent_threshold_ >= cleaner_threshold_);
  ASSERT_ND(total_pages_ >= urgent_threshold_);

  reclaimed_pages_memory_.alloc(
    total_pages_ * sizeof(memory::PagePoolOffset),
    1ULL << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    engine_->get_soc_id());
  reclaimed_pages_ = reinterpret_cast<memory::PagePoolOffset*>(reclaimed_pages_memory_.get_block());

  // launch the cleaner thread
  stop_requested_.store(false);
  cleaner_ = std::move(std::thread(&CacheManagerPimpl::handle_cleaner, this));

  return kRetOk;
}

ErrorStack CacheManagerPimpl::uninitialize_once() {
  if (engine_->is_master()) {
    // nothing to do in master engine
    return kRetOk;
  }

  LOG(INFO) << "Uninitializing Snapshot Cache... " << describe();
  stop_requested_.store(true);
  if (cleaner_.joinable()) {
    cleaner_.join();
  }

  pool_ = nullptr;
  hashtable_ = nullptr;
  reclaimed_pages_ = nullptr;
  reclaimed_pages_memory_.release_block();
  reclaimed_pages_count_ = 0;
  return kRetOk;
}

void CacheManagerPimpl::handle_cleaner() {
  LOG(INFO) << "Here we go. Cleaner thread: " << describe();

  const uint32_t kIntervalMs = 5;  // should be a bit shorter than epoch-advance interval
  while (!stop_requested_) {
    DVLOG(1) << "Cleaner thread came in: " << describe();
    ASSERT_ND(reclaimed_pages_count_ == 0);
    assorted::memory_fence_acquire();
    memory::PagePool::Stat stat = pool_->get_stat();
    ASSERT_ND(stat.total_pages_ == total_pages_);
    if (stat.allocated_pages_ > cleaner_threshold_) {
      VLOG(0) << "Time to evict: " << describe();

      uint64_t target_count = stat.allocated_pages_ - cleaner_threshold_;
      debugging::StopWatch evict_watch;
      handle_cleaner_evict_pages(target_count);
      evict_watch.stop();
      VLOG(0) << "Evicted " << reclaimed_pages_count_ << " pages in " << evict_watch.elapsed_us()
        << "us: " << describe();

      if (reclaimed_pages_count_ > 0) {
        // We collected some number of pages in previous execution. now we have to wait for
        // grace period befire returning them to the pool as free pages.
        ASSERT_ND(engine_->get_xct_manager()->is_initialized());  // otherwise why already collected
        Epoch reclaimed_pages_epoch = engine_->get_xct_manager()->get_current_global_epoch();
        VLOG(0) << "Collected " << reclaimed_pages_count_ << " pages. let's return them to "
          << "the pool: " << describe();
        Epoch wait_until = reclaimed_pages_epoch.one_more();

        // We have to wait for grace-period, in other words until the next epoch.
        if (stat.allocated_pages_ >= urgent_threshold_) {
          VLOG(0) << "We are in urgent lack of free pages, let's advance epoch right now";
          engine_->get_xct_manager()->advance_current_global_epoch();
        }
        engine_->get_xct_manager()->wait_for_current_global_epoch(wait_until);

        Epoch current_epoch = engine_->get_xct_manager()->get_current_global_epoch();
        ASSERT_ND(reclaimed_pages_epoch < current_epoch);
        VLOG(0) << "Okay! reclaimed_pages_epoch_=" << reclaimed_pages_epoch
          << ", current_epoch=" << current_epoch;

        memory::PagePoolOffsetDynamicChunk chunk(reclaimed_pages_count_, reclaimed_pages_);
        pool_->release(reclaimed_pages_count_, &chunk);
        reclaimed_pages_count_ = 0;
      } else {
        LOG(INFO) << "Wtf, we couldn't collect any pages? that's weird...: " << describe();
      }

      if (stat.allocated_pages_ >= urgent_threshold_) {
        LOG(INFO) << "Umm, still severely lacking free pages. This might mean"
          " that the cleaner thread is getting behind or the pool is too small: " << describe();
      }
    } else {
      DVLOG(1) << "Still enough free pages. do nothing";
    }

    if (!stop_requested_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(kIntervalMs));
    }
  }

  LOG(INFO) << "Quiting... Cleaner thread: " << describe();
}

void CacheManagerPimpl::handle_cleaner_evict_pages(uint64_t target_count) {
  ASSERT_ND(reclaimed_pages_count_ == 0);
  ASSERT_ND(target_count > 0);
}

std::string CacheManagerPimpl::describe() const {
  if (pool_ == nullptr) {
    return "<SnapshotCacheManager />";
  }

  std::stringstream str;
  memory::PagePool::Stat pool_stat = pool_->get_stat();
  str << "<SpCache "
    << " node=\"" << engine_->get_soc_id() << "\""
    << " total=\"" << pool_stat.total_pages_ << "\""
    << " alloc=\"" << pool_stat.allocated_pages_ << "\""
    << " threshold=\"" << cleaner_threshold_ << "\""
    << " urgent_threshold=\"" << urgent_threshold_ << "\""
    << " reclaimed_count=\"" << reclaimed_pages_count_ << "\""
    << ">" << reclaimed_pages_memory_ << "</SpCache>";
  return str.str();
}



}  // namespace cache
}  // namespace foedus
