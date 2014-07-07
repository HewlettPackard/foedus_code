/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/cache/cache_options.hpp"

#include "foedus/memory/page_pool.hpp"

namespace foedus {
namespace cache {
CacheOptions::CacheOptions() {
  snapshot_cache_enabled_ = true;
  snapshot_cache_size_mb_per_node_ = kDefaultSnapshotCacheSizeMbPerNode;
  private_snapshot_cache_initial_grab_ = memory::PagePoolOffsetChunk::kMaxSize / 2;
}
ErrorStack CacheOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_cache_enabled_);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_cache_size_mb_per_node_);
  EXTERNALIZE_LOAD_ELEMENT(element, private_snapshot_cache_initial_grab_);
  return kRetOk;
}
ErrorStack CacheOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for snapshot cache manager."));
  EXTERNALIZE_SAVE_ELEMENT(element, snapshot_cache_enabled_,
    "Whether to cache the read accesses on snapshot files.");
  EXTERNALIZE_SAVE_ELEMENT(element, snapshot_cache_size_mb_per_node_,
    "Size of the snapshot cache in MB per each NUMA node.");
  EXTERNALIZE_SAVE_ELEMENT(element, private_snapshot_cache_initial_grab_,
    "How many pages for snapshot cache each NumaCoreMemory initially grabs"
    " when it is initialized.");
  return kRetOk;
}

}  // namespace cache
}  // namespace foedus
