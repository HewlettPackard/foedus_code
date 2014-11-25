/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_OPTIONS_HPP_
#define FOEDUS_CACHE_CACHE_OPTIONS_HPP_
#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"
namespace foedus {
namespace cache {
/**
 * @brief Set of options for snapshot cache manager.
 * @ingroup CACHE
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct CacheOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /** Constant values. */
  enum Constants {
    /** Default value for snapshot_cache_size_mb_per_node_. */
    kDefaultSnapshotCacheSizeMbPerNode = 1 << 10,
  };

  /**
   * Constructs option values with default values.
   */
  CacheOptions();

  /**
   * @brief Whether to cache the read accesses on snapshot files.
   * @details
   * If the storage media is 2x or more slower than DRAM, we should cache it, but otherwise
   * we should turn it off and directly read from snapshot files on it.
   * This property turns on/off the entire snapshot cache module (remember, snapshot cache
   * is a totally optional module unlike bufferpools in traditional databases).
   * Default is ON.
   */
  bool        snapshot_cache_enabled_;

  /**
   * @brief Size of the snapshot cache in MB per each NUMA node.
   * @details
   * Must be multiply of 2MB. Default is 1GB.
   * The total amount of memory is page_pool_size_mb_per_node_ *
   */
  uint32_t    snapshot_cache_size_mb_per_node_;

  /**
   * @brief How many pages for snapshot cache each NumaCoreMemory initially grabs
   * when it is initialized.
   * @details
   * Default is 50% of PagePoolOffsetChunk::MAX_SIZE.
   * Obviously, private_snapshot_cache_initial_grab_ * kPageSize * number-of-threads-per-node must
   * be within snapshot_cache_size_mb_per_node_ to start up the engine.
   */
  uint32_t    private_snapshot_cache_initial_grab_;

  /**
   * @brief When to start evicting pages in fraction of snapshot page pool capacity.
   * @invariant between (0, 1)
   * @details
   * This is an important tuning parameter.
   * \b If the snapshot page pool is sufficiently large and the cleaner can perfectly catch
   * up with the speed of free-page consumption by worker threads, this can be 99% to maximize
   * the use of DRAM.
   * However, in most cases the assumptions do not hold, so this should be something like 75%.
   * If it's too large, you will see no-free-pages error in transaction executions.
   * If it's too small, you get more cache misses due to utilizing less DRAM.
   * Probably the latter is less problemetic.
   */
  float       snapshot_cache_eviction_threshold_;

  /**
   * @brief When the cache eviction performs in an urgent mode, which immediately advances
   * the current epoch to release pages.
   * @invariant between [snapshot_cache_eviction_threshold_, 1]
   * @details
   * We shouldn't advance current epoch too often as it might run out of the value space
   * and we might need synchronization with worker threads to maintain "low water mark".
   * So, the cleaner usually just passively waits for the new epoch while the grace period.
   * When the system is really running out of free pages, we should do something else.
   * This parameter specifies when to do that.
   */
  float       snapshot_cache_urgent_threshold_;

  EXTERNALIZABLE(CacheOptions);
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_OPTIONS_HPP_
