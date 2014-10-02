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
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/page.hpp"
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
class SnapshotWriter final : public DefaultInitializable {
 public:
  SnapshotWriter(Engine* engine, uint16_t numa_node);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;
  /**
   * Close the file and makes sure all writes become durable (including the directory entry).
   * @return whether successfully closed and synced.
   */
  bool        close();

  SnapshotWriter() = delete;
  SnapshotWriter(const SnapshotWriter &other) = delete;
  SnapshotWriter& operator=(const SnapshotWriter &other) = delete;


  uint16_t                get_numa_node() const { return numa_node_; }
  storage::Page*          get_page_base() { return page_base_; }
  memory::PagePoolOffset  get_page_size() const { return pool_size_;}
  storage::Page*          get_intermediate_base() { return intermediate_base_; }
  memory::PagePoolOffset  get_intermediate_size() const { return intermediate_size_;}
  storage::SnapshotPagePointer get_next_page_id() const { return next_page_id_;}
  SnapshotId              get_snapshot_id() const { return snapshot_id_; }

  storage::Page*          resolve(memory::PagePoolOffset offset) ALWAYS_INLINE {
    ASSERT_ND(offset > 0);
    ASSERT_ND(offset < pool_size_);
    return page_base_ + offset;
  }
  memory::PagePoolOffset  resolve(storage::Page* page) ALWAYS_INLINE {
    memory::PagePoolOffset offset = page - page_base_;
    ASSERT_ND(offset > 0);
    ASSERT_ND(offset < pool_size_);
    return offset;
  }

  /**
   * @brief Write out pages that are contiguous in the main page pool.
   * @param[in] from_page beginning of contiguous in-memory pages to fix
   * @param[in] count number of pages to write out
   */
  ErrorCode dump_pages(memory::PagePoolOffset from_page, uint32_t count) {
    return dump_general(&pool_memory_, from_page, count);
  }

  /**
   * @brief Write out pages that are contiguous in the sub intermediate page pool.
   * @param[in] from_page beginning of contiguous in-memory pages to fix
   * @param[in] count number of pages to write out
   */
  ErrorCode dump_intermediates(memory::PagePoolOffset from_page, uint32_t count) {
    return dump_general(&intermediate_memory_, from_page, count);
  }

  std::string             to_string() const {
    return "SnapshotWriter-" + std::to_string(numa_node_);
  }
  friend std::ostream&    operator<<(std::ostream& o, const SnapshotWriter& v);

 private:
  Engine* const                   engine_;
  /** NUMA node for allocated memories. */
  const uint16_t                  numa_node_;
  /** Id of the snapshot this is currently writing for. */
  SnapshotId                      snapshot_id_;

  /** The snapshot file to write to. */
  fs::DirectIoFile*               snapshot_file_;

  /** This is the main page pool for all composers using this snapshot writer. */
  memory::AlignedMemory           pool_memory_;
  /** Same as pool_memory_.get_block(). */
  storage::Page*                  page_base_;
  /** Size of the pool in pages. */
  memory::PagePoolOffset          pool_size_;

  /**
   * This is the sub page pool for intermdiate pages (main one is for leaf pages).
   * We separate out intermediate pages and assume that this pool can hold all
   * intermediate pages modified in one compose() while we might flush pool_memory_
   * multiple times for one compose().
   */
  memory::AlignedMemory           intermediate_memory_;
  /** Same as intermediate_memory_.get_block(). */
  storage::Page*                  intermediate_base_;
  /** Size of the intermediate_memory_ in pages. */
  memory::PagePoolOffset          intermediate_size_;

  /**
   * The page that is written next will correspond to this snapshot page ID.
   */
  storage::SnapshotPagePointer    next_page_id_;

  void      clear_snapshot_file();
  fs::Path  get_snapshot_file_path() const;
  ErrorCode dump_general(
    memory::AlignedMemory* memory,
    memory::PagePoolOffset from_page,
    uint32_t count);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
