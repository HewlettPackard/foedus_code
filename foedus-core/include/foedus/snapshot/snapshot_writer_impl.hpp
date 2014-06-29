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
#include "foedus/memory/page_pool.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
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
 * Here, snapshot writers behave just a usual in-memory page pool.
 * This part depends on composer, so the snapshot writer calls composer's method.
 *
 * @par Fix Phase
 * Next phase is invoked at the end of composer for the storage, finalizing page ID in the snapshot
 * file for each modified page and replacing volatile page pointers with snapshot pointers.
 * This part also depends on composer (or page format of the storage), so the snapshot writer calls
 * composer's method.
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
  SnapshotWriter(Engine* engine, LogReducer* parent);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  SnapshotWriter() = delete;
  SnapshotWriter(const SnapshotWriter &other) = delete;
  SnapshotWriter& operator=(const SnapshotWriter &other) = delete;

  const memory::PagePool&                get_pool() const { return page_pool_; }
  const memory::LocalPageResolver&       get_resolver() const { return page_resolver_; }

  memory::PagePoolOffset allocate_new_page() ALWAYS_INLINE {
    if (UNLIKELY(free_page_chunk_.empty())) {
      // composers don't deallocate each page, so it's best to make it full each time.
      ErrorCode ret = page_pool_.grab(memory::PagePoolOffsetChunk::kMaxSize, &free_page_chunk_);
      if (ret != kErrorCodeOk) {
        // no more free pages, now we are going to flush all pages except right-most pages
        return 0;  // caller checks this.
      }
    }
    ASSERT_ND(!free_page_chunk_.empty());
    return free_page_chunk_.pop_back();
  }

  /**
   * @brief Maps given in-memory pages to page IDs in the snapshot file.
   * @param[in] memory_pages in-memory pages to fix
   * @param[in] count length of memory_pages
   * @return the base local page ID, or the page ID of memory_pages[0] when it is written to a file.
   * All the following pages in memory_pages get contiguous page IDs, so
   * memory_pages[3]'s page ID is returned_value + 3.
   * @details
   * This is called by composers to obtain page IDs in the file when it finishes composing
   * the pages. Receiving the base page ID, composers will finalize their data pages to replace
   * page IDs in data pages. When it's done, they will call dump_pages().
   */
  storage::SnapshotLocalPageId fix_pages(
    const memory::PagePoolOffset* memory_pages,
    uint32_t count);

  /**
   * @brief Writes out in-memory pages to the snapshot file.
   * @param[in] memory_pages in-memory pages to fix
   * @param[in] count length of memory_pages
   * @details
   * All pages will be written contiguously. So, this method first stitches the in-memory pages
   * to IO buffer then call write(). We do so even if the in-memory pages are (luckily) contiguous.
   */
  ErrorCode dump_pages(const memory::PagePoolOffset* memory_pages, uint32_t count);

  /**
   * @brief Called when one storage is fully or partially written.
   * @details
   * Returns all in-memory pages to the pool \b except the excluded pages.
   * The excluded pages are given only when the storage is partially written to avoid OOM.
   * This is the only interface in snapshot writer to return pages to pool.
   * Compared to releasing each page, this is much more efficient.
   */
  void      reset_pool(const memory::PagePoolOffset* excluded_pages, uint32_t excluded_count);

  /** for recycling dump_io_buffer. */
  void      set_dump_io_buffer(memory::AlignedMemory* dump_io_buffer) {
    dump_io_buffer_ = dump_io_buffer;
  }

  std::string             to_string() const {
    return "SnapshotWriter-" + std::to_string(numa_node_);
  }
  friend std::ostream&    operator<<(std::ostream& o, const SnapshotWriter& v);

 private:
  Engine* const                   engine_;
  LogReducer* const               parent_;
  /** Also parent's ID. One NUMA node = one reducer = one snapshot writer. */
  const thread::ThreadGroupId     numa_node_;
  /** Same as parent_->get_parent()->get_snapshot()->id_. Stored for convenience. */
  const SnapshotId                snapshot_id_;

  /** The snapshot file to write to. */
  fs::DirectIoFile*               snapshot_file_;

  /** This is the only page pool for all composers using this snapshot writer. */
  memory::PagePool                page_pool_;
  /** so, there is no global page resolver. Easy and efficient. */
  memory::LocalPageResolver       page_resolver_;
  /** Locally cached free pages. */
  memory::PagePoolOffsetChunk     free_page_chunk_;

  /**
   * Used to sequentially write out data pages to a file.
   * The writer does NOT own this buffer. It's actually a second-hand buffer given by
   * reducer (was reducer's dump IO buffer).
   */
  memory::AlignedMemory*          dump_io_buffer_;

  /**
   * This writer has fixed pages up to this number.
   * In other word, the next page will be fixed_upto_ + 1.
   */
  storage::SnapshotLocalPageId    fixed_upto_;

  void      clear_snapshot_file();
  fs::Path  get_snapshot_file_path() const;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
