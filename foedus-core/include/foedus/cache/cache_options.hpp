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

  EXTERNALIZABLE(CacheOptions);
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_OPTIONS_HPP_
