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
#ifndef FOEDUS_CACHE_CACHE_MANAGER_PIMPL_HPP_
#define FOEDUS_CACHE_CACHE_MANAGER_PIMPL_HPP_

#include <atomic>
#include <string>
#include <thread>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/page_pool.hpp"

namespace foedus {
namespace cache {
/**
 * @brief Pimpl object of CacheManager.
 * @ingroup CACHE
 * @details
 * A private pimpl object for CacheManager.
 * Do not include this header from a client program unless you know what you are doing.
 *
 * @par Eviction Policy
 * So far we use a simple CLOCK algorithm to minimize the overhead, especially synchronization
 * overhead.
 */
class CacheManagerPimpl final : public DefaultInitializable {
 public:
  CacheManagerPimpl() = delete;
  explicit CacheManagerPimpl(Engine* engine);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  std::string describe() const;

  /** Main routine of cleaner_ */
  void        handle_cleaner();
  /**
   * Evicts pages up to about target_count (maybe a bit more or less).
   * Results are written to reclaimed_pages_count_ and reclaimed_pages_.
   */
  void        handle_cleaner_evict_pages(uint64_t target_count);

  Engine* const     engine_;

  /**
   * @brief The only cleaner thread in this SOC engine.
   * @details
   * In a master engine, this is not used.
   */
  std::thread       cleaner_;
  uint64_t          total_pages_;
  /** the number of allocated pages above which cleaner starts cleaning */
  uint64_t          cleaner_threshold_;
  /** the number of allocated pages above which cleaner advances epoch to release pages */
  uint64_t          urgent_threshold_;
  /** to stop cleaner_ */
  std::atomic<bool> stop_requested_;

  /**
   * @brief The SOC-local snapshot page pool in this SOC engine.
   * @details
   * This passively (no thread by itself) holds used pages and free pages.
   * In a master engine, this is null.
   */
  memory::PagePool* pool_;

  /**
   * @brief The cache hashtable (SnapshotPageId -> offset in pool) on top of the page pool.
   * @details
   * This cache manager keeps checking
   * In a master engine, this is null.
   */
  CacheHashtable*   hashtable_;

  /**
   * @brief This buffers pages being reclaimed.
   * @details
   * We reclaim pages after a grace period, rather than immediately after collecting them
   * from the hashtable. Because of this epoch-based \e grace-period, we don't have to worry
   * about thread-safety. This is super benefitial for simplicity and scalability.
   * Because snapshot pages are immutable, that's all we have to do.
   * Whereas, traditional bufferpools have to take care of dirty pages and singularity of page,
   * so they can't just do grace-period.
   * So far, the grace period is one epoch.
   */
  memory::PagePoolOffset* reclaimed_pages_;

  /** The memory backing reclaimed_pages_. */
  memory::AlignedMemory reclaimed_pages_memory_;

  /** Number of pages buffered so far. */
  uint64_t  reclaimed_pages_count_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_MANAGER_PIMPL_HPP_
