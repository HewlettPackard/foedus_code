/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief Writes out one snapshot file for all data pages in one reducer.
 * @ingroup SNAPSHOT
 * @details
 * In a nutshell, snapshot writer is a in-memory page pool that dumps out some or all of the
 * pages to a snapshot file. It consists of 3 phases \e for \e each \e storage.
 *
 * @par Compose Phase
 * This first phase is invoked by the composers, loading prior snapshot pages and modifying them.
 * Here, snapshot writers behave just as a usual in-memory page pool.
 * This part depends on composer, so the snapshot writer calls composer's method.
 *
 * @par Fix Phase
 * Next phase is invoked at the end of composer for the storage, finalizing page ID in the snapshot
 * file for each modified page and replacing volatile page pointers with snapshot pointers.
 * This part also depends on composer (or page format of the storage), so this is done by
 * composer.
 *
 * @par Dump Phase
 * The last phase simply dumps out the pages to snapshot file. This is a sequential write
 * because no two storages have overlapping pages.
 * This is independent from storage type, thus done in snapshot writer.
 *
 * @par Conquer already-divided
 * Snapshot writer might not have enough pages to hold all pages of the storage modified in this
 * snapshot. This can happen for a large storage with lots of changes.
 * No worry, we have already sorted log entries by keys for this reason.
 * When the page pool becomes fully occupied, we go on to the fix/dump phase, only keeping the
 * \b right-most pages in all levels. After dumping everything else, we repeat the compose phase
 * just like moving on to another storage.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class SnapshotWriter final {
 public:
  SnapshotWriter(
    Engine* engine,
    uint16_t numa_node,
    SnapshotId snapshot_id,
    memory::AlignedMemory* pool_memory,
    memory::AlignedMemory* intermediate_memory,
    bool append = false);
  ~SnapshotWriter() { close(); }

  /** Open the file so that the writer can start writing. */
  ErrorStack  open();
  bool        is_opened() const { return snapshot_file_; }
  /**
   * Close the file and makes sure all writes become durable (including the directory entry).
   * @return whether successfully closed and synced.
   */
  bool        close();

  SnapshotWriter() = delete;
  SnapshotWriter(const SnapshotWriter &other) = delete;
  SnapshotWriter& operator=(const SnapshotWriter &other) = delete;


  uint16_t                get_numa_node() const { return numa_node_; }
  inline storage::Page*   get_page_base() ALWAYS_INLINE {
    return reinterpret_cast<storage::Page*>(pool_memory_->get_block());
  }
  inline memory::PagePoolOffset  get_page_size() const ALWAYS_INLINE {
    return pool_memory_->get_size() / storage::kPageSize;
  }
  inline storage::Page*          get_intermediate_base() ALWAYS_INLINE {
    return reinterpret_cast<storage::Page*>(intermediate_memory_->get_block());
  }
  inline memory::PagePoolOffset  get_intermediate_size() const ALWAYS_INLINE {
    return intermediate_memory_->get_size() / storage::kPageSize;
  }

  storage::SnapshotPagePointer get_next_page_id() const { return next_page_id_; }
  SnapshotId              get_snapshot_id() const { return snapshot_id_; }

  storage::Page*          resolve(memory::PagePoolOffset offset) ALWAYS_INLINE {
    ASSERT_ND(offset > 0);
    ASSERT_ND(offset < get_page_size());
    return get_page_base() + offset;
  }
  memory::PagePoolOffset  resolve(storage::Page* page) ALWAYS_INLINE {
    memory::PagePoolOffset offset = page - get_page_base();
    ASSERT_ND(offset > 0);
    ASSERT_ND(offset < get_page_size());
    return offset;
  }

  /**
   * @brief Write out pages that are contiguous in the main page pool.
   * @param[in] from_page beginning of contiguous in-memory pages to fix
   * @param[in] count number of pages to write out
   */
  ErrorCode dump_pages(memory::PagePoolOffset from_page, uint32_t count) {
    return dump_general(pool_memory_, from_page, count);
  }

  /**
   * @brief Write out pages that are contiguous in the sub intermediate page pool.
   * @param[in] from_page beginning of contiguous in-memory pages to fix
   * @param[in] count number of pages to write out
   */
  ErrorCode dump_intermediates(memory::PagePoolOffset from_page, uint32_t count) {
    return dump_general(intermediate_memory_, from_page, count);
  }

  std::string             to_string() const {
    return "SnapshotWriter-" + std::to_string(numa_node_) + (append_ ? "(append)" : "");
  }

  /**
   * @brief Expands pool_memory_ in case it is too small.
   * @param[in] required_pages number of pages that are guaranteed after expansion. The actual
   * size will probably be larger than this value to avoid frequent expansion.
   * @param[in] retain_content whether to copy the content of old memory to new memory
   * @post get_page_size() >= required_pages
   * @return only possible error is out of memory
   * @note page_base will be also changed, so do not forget to re-obtain it.
   * @details
   * Does nothing if it's already enough large.
   * Most likely we don't have to use this method as composers should be able to run even when
   * the number of leaf (content) pages is large.
   */
  ErrorCode expand_pool_memory(uint32_t required_pages, bool retain_content);

  /**
   * @brief Expands intermediate_memory_ in case it is too small.
   * @param[in] required_pages number of pages that are guaranteed after expansion. The actual
   * size will probably be larger than this value to avoid frequent expansion.
   * @param[in] retain_content whether to copy the content of old memory to new memory
   * @post get_intermediate_size() >= required_pages
   * @return only possible error is out of memory
   * @note intermediate_base will be also changed, so do not forget to re-obtain it.
   * @details
   * Does nothing if it's already enough large.
   * We use this method to make sure intermediate pool (which some composer assumes sufficiently
   * large) can contain all intermediate pages.
   */
  ErrorCode expand_intermediate_memory(uint32_t required_pages, bool retain_content);

  friend std::ostream&    operator<<(std::ostream& o, const SnapshotWriter& v);

 private:
  Engine* const                   engine_;
  /** NUMA node for allocated memories. */
  const uint16_t                  numa_node_;
  /** Whether we are appending to an existing file. */
  const bool                      append_;
  /** ID of the snapshot this writer is currently working on. */
  const SnapshotId                snapshot_id_;

  /**
   * This is the main page pool for all composers using this snapshot writer.
   * Snapshot writer expands this memory in expand_pool_memory().
   */
  memory::AlignedMemory* const    pool_memory_;

  /**
   * This is the sub page pool for intermdiate pages (main one is for leaf pages).
   * We separate out intermediate pages and assume that this pool can hold all
   * intermediate pages modified in one compose() while we might flush pool_memory_
   * multiple times for one compose().
   * Snapshot writer expands this memory in expand_intermediate_memory().
   */
  memory::AlignedMemory* const    intermediate_memory_;

  /** The snapshot file to write to. */
  fs::DirectIoFile*               snapshot_file_;

  /**
   * The page that is written next will correspond to this snapshot page ID.
   */
  storage::SnapshotPagePointer    next_page_id_;

  fs::Path  get_snapshot_file_path() const;
  ErrorCode dump_general(
    memory::AlignedMemory* buffer,
    memory::PagePoolOffset from_page,
    uint32_t count);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
