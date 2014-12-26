/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_COMPOSER_HPP_
#define FOEDUS_STORAGE_COMPOSER_HPP_

#include <iosfwd>
#include <string>

#include "foedus/compiler.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace storage {
/**
 * @brief Represents a logic to compose a new version of data pages for one storage.
 * @ingroup STORAGE SNAPSHOT
 * @details
 * @section COMPOSER_OVERVIEW Overview
 * This object is one of the liaisons between \ref SNAPSHOT module and \ref STORAGE module.
 * It receives previous snapshot files and pre-sorted log entries from snapshot module,
 * then applies a storage-specific implementation to convert them into a new version of data pages.
 * Every interface is batched and completely separated from the normal transactional processing
 * part. In fact, this object is not part of foedus::storage::Storage at all.
 *
 * @section COMPOSER_SCOPE Composer's scope
 * One composer object is in charge of data pages that meet \b all of following criteria:
 *  \li In one storage
 *  \li In one partition (in one NUMA node)
 *  \li In one snapshot
 *
 * None of these responsibilities is overlapping, so the job of composer is totally independent
 * from other composers \b except the root page of the storage.
 *
 * @section COMPOSER_INPUTS Inputs
 * Every composer receives the following when constructed.
 *  \li Corresponding Partitioner object that tells what pages this composer is responsible for.
 *  \li Pre-allocated and reused working memory (assured to be on the same NUMA node).
 *  \li Pre-sorted stream(s) of log entries (foedus::snapshot::SortedBuffer).
 *  \li Snapshot writer to allocate pages and write them out to a snapshot file.
 *  \li Most recent snapshot files.
 *
 * @section COMPOSER_OUTPUTS Outputs
 * Composers emit the following data when it's done.
 *  \li Composed data pages, which are written to the snapshot file by the snapshot writer.
 *  \li For each storage and for each second-level page that is pointed from the root page,
 * the snapshot pointer and relevant pointer information (eg key range).
 * We call this information as \e root-info and store them in a tentative page.
 * This is required to construct the root page at the end of snapshotting.
 *
 * @section COMPOSER_INSTALL Installing Composed Pages
 * At the end of snapshotting, composers install pointers to the snapshot pages they composed.
 * These are written to the snapshot pointer part of DualPagePointer so that transactions
 * can start using the snapshot pages.
 * Composers also drop volatile pointers if possible, reducing pressures to volatile page pool.
 *
 * @par Shared memory, No virtual methods
 * Like Partitioner, no virtual methods allowed. We just do switch-case.
 */
class Composer CXX11_FINAL {
 public:
  Composer(Engine *engine, StorageId storage_id);

  Engine*   get_engine() { return engine_; }
  StorageId get_storage_id() const { return storage_id_; }
  StorageType get_storage_type() const { return storage_type_; }

  /** Arguments for compose() */
  struct ComposeArguments {
    /** Writes out composed pages. */
    snapshot::SnapshotWriter*         snapshot_writer_;
    /** To read existing snapshots. */
    cache::SnapshotFileSet*           previous_snapshot_files_;
    /** Sorted runs. */
    snapshot::SortedBuffer* const*    log_streams_;
    /** Number of sorted runs. */
    uint32_t                          log_streams_count_;
    /** Working memory to be used in this method. Automatically expand if needed. */
    memory::AlignedMemory*            work_memory_;
    /**
     * [OUT] Returns pointers and related information that is required
     * to construct the root page. The data format depends on the composer. In all implementations,
     * the information must fit in one page (should be, otherwise we can't have a root page)
     */
    Page*                             root_info_page_;
  };
  /**
   * @brief Construct snapshot pages from sorted run files of one storage.
   */
  ErrorStack  compose(const ComposeArguments& args);

  /** Arguments for construct_root() */
  struct ConstructRootArguments {
    /** Writes out composed pages. */
    snapshot::SnapshotWriter*         snapshot_writer_;
    /** To read existing snapshots. */
    cache::SnapshotFileSet*           previous_snapshot_files_;
    /** Root info pages output by compose() */
    const Page* const*                root_info_pages_;
    /** Number of root info pages. */
    uint32_t                          root_info_pages_count_;
    /** Working memory to be used in this method. Automatically expand if needed. */
    memory::AlignedMemory*            work_memory_;
    /** [OUT] Returns pointer to new root snapshot page/ */
    SnapshotPagePointer*              new_root_page_pointer_;
  };

  /**
   * @brief Construct root page(s) for one storage based on the ouputs of compose().
   * @details
   * When all reducers complete, the gleaner invokes this method to construct new root
   * page(s) for the storage. This
   */
  ErrorStack  construct_root(const ConstructRootArguments& args);

  /** Arguments for replace_pointers() */
  struct ReplacePointersArguments {
    /** The new snapshot. All newly created snapshot pages are of this snapshot */
    snapshot::Snapshot            snapshot_;
    /** To read the new snapshot. */
    cache::SnapshotFileSet*       snapshot_files_;
    /** Pointer to new root snapshot page */
    SnapshotPagePointer           new_root_page_pointer_;
    /** Working memory to be used in this method. Automatically expand if needed. */
    memory::AlignedMemory*        work_memory_;
    /**
     * Caches dropped pages to avoid returning every single page.
     * This is an array of PagePoolOffsetChunk whose index is node ID.
     * For each dropped page, we add it to this chunk and batch-return them to the
     * volatile pool when it becomes full or after processing all storages.
     */
    memory::PagePoolOffsetChunk*  dropped_chunks_;
    /** [OUT] Number of snapshot pages that were installed */
    uint64_t*                     installed_count_;
    /** [OUT] Number of volatile pages that were dropped */
    uint64_t*                     dropped_count_;

    /**
     * Returns (might cache) the given pointer to volatile pool.
     */
    void drop_volatile_page(VolatilePagePointer pointer) const;
    /** Same as snapshot_files_->read_page(pointer, out) */
    ErrorCode read_snapshot_page(SnapshotPagePointer pointer, void* out) const;
  };

  /**
   * @brief Installs new snapshot pages and drops volatile pages that
   * have not been modified since the snapshotted epoch.
   * @details
   * This is called after pausing transaction executions, so this method does not worry about
   * concurrent reads/writes while running this. Otherwise this method becomes
   * very complex and/or expensive. It's just milliseconds for each several minutes, so should
   * be fine to pause transactions.
   * Also, this method is best-effort in many aspects. It might not drop some volatile pages
   * that were not logically modified, or it might not install some of snapshot pages to pointers
   * in volatile pages. In long run, it will be done at next snapshot,
   * so it's okay to be opportunistic.
   */
  ErrorStack  replace_pointers(const ReplacePointersArguments& args);

  friend std::ostream&    operator<<(std::ostream& o, const Composer& v);

 private:
  Engine* const                       engine_;
  const StorageId                     storage_id_;
  const StorageType                   storage_type_;
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_COMPOSER_HPP_
