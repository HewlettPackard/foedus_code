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
#include "foedus/cache/cache_options.hpp"

#include "foedus/memory/page_pool.hpp"

namespace foedus {
namespace cache {
CacheOptions::CacheOptions() {
  snapshot_cache_enabled_ = true;
  snapshot_cache_size_mb_per_node_ = kDefaultSnapshotCacheSizeMbPerNode;
  private_snapshot_cache_initial_grab_ = memory::PagePoolOffsetChunk::kMaxSize / 2;
  snapshot_cache_eviction_threshold_ = 0.75;
  snapshot_cache_urgent_threshold_ = 0.9;
}
ErrorStack CacheOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_cache_enabled_);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_cache_size_mb_per_node_);
  EXTERNALIZE_LOAD_ELEMENT(element, private_snapshot_cache_initial_grab_);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_cache_eviction_threshold_);
  ASSERT_ND(snapshot_cache_eviction_threshold_ > 0);
  ASSERT_ND(snapshot_cache_eviction_threshold_ < 1);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_cache_urgent_threshold_);
  ASSERT_ND(snapshot_cache_urgent_threshold_ >= snapshot_cache_eviction_threshold_);
  ASSERT_ND(snapshot_cache_urgent_threshold_ <= 1);
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
  ASSERT_ND(snapshot_cache_eviction_threshold_ > 0);
  ASSERT_ND(snapshot_cache_eviction_threshold_ < 1);
  EXTERNALIZE_SAVE_ELEMENT(
    element,
    snapshot_cache_eviction_threshold_,
    "When to start evicting pages in fraction of snapshot page pool capacity");
  ASSERT_ND(snapshot_cache_urgent_threshold_ >= snapshot_cache_eviction_threshold_);
  ASSERT_ND(snapshot_cache_urgent_threshold_ <= 1);
  EXTERNALIZE_SAVE_ELEMENT(
    element,
    snapshot_cache_urgent_threshold_,
    "When the cache eviction performs in an urgent mode, which immediately advances"
    " the current epoch to release pages");
  return kRetOk;
}

}  // namespace cache
}  // namespace foedus
