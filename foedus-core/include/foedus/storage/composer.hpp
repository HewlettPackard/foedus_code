/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_COMPOSER_HPP_
#define FOEDUS_STORAGE_COMPOSER_HPP_

#include <iosfwd>
#include <string>

#include "foedus/compiler.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
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
 */
class Composer {
 public:
  Composer(
    Engine *engine,
    const Partitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    cache::SnapshotFileSet* previous_snapshot_files,
    const snapshot::Snapshot& new_snapshot);
  virtual ~Composer() {}

  /** Returns a short string that briefly describes this object. */
  virtual std::string to_string() const = 0;

  /** Writes out a detailed description of this object to stream. */
  virtual void        describe(std::ostream* o) const = 0;

  /** Returns the size of working memory this composer needs. */
  virtual uint64_t    get_required_work_memory_size(
    snapshot::SortedBuffer**  log_streams,
    uint32_t                  log_streams_count) const = 0;

  /**
   * @brief Construct snapshot pages from sorted run files of one storage.
   * @param[in] log_streams Sorted runs
   * @param[in] log_streams_count Number of sorted runs
   * @param[in] work_memory Working memory to be used in this method
   * @param[out] root_info_page Returns pointers and related information that is required
   * to construct the root page. The data format depends on the composer. In all implementations,
   * the information must fit in one page (should be, otherwise we can't have a root page)
   */
  virtual ErrorStack  compose(
    snapshot::SortedBuffer* const*    log_streams,
    uint32_t                          log_streams_count,
    const memory::AlignedMemorySlice& work_memory,
    Page*                             root_info_page) = 0;

  /**
   * @brief Construct root page(s) for one storage based on the ouputs of compose().
   * @param[in] root_info_pages Root info pages output by compose()
   * @param[in] root_info_pages_count Number of root info pages.
   * @param[in] work_memory Working memory to be used in this method
   * @param[out] new_root_page_pointer Returns pointer to new root snapshot page
   * @details
   * When all reducers complete, the gleaner invokes this method to construct new root
   * page(s) for the storage. This
   */
  virtual ErrorStack  construct_root(
    const Page* const*  root_info_pages,
    uint32_t            root_info_pages_count,
    const memory::AlignedMemorySlice& work_memory,
    SnapshotPagePointer* new_root_page_pointer) = 0;

  /** factory method. */
  static Composer*    create_composer(
    Engine *engine,
    const Partitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    cache::SnapshotFileSet* previous_snapshot_files,
    const snapshot::Snapshot& new_snapshot);

  friend std::ostream&    operator<<(std::ostream& o, const Composer& v);

 protected:
  Engine* const                       engine_;
  const Partitioner* const            partitioner_;
  snapshot::SnapshotWriter* const     snapshot_writer_;
  cache::SnapshotFileSet* const       previous_snapshot_files_;
  const snapshot::Snapshot&           new_snapshot_;
  const snapshot::SnapshotId          new_snapshot_id_;
  const StorageId                     storage_id_;
  const thread::ThreadGroupId         numa_node_;
  StorageControlBlock* const          storage_;
  const SnapshotPagePointer           previous_root_page_pointer_;

  inline SnapshotPagePointer to_snapshot_pointer(SnapshotLocalPageId local_id) const ALWAYS_INLINE {
    return foedus::storage::to_snapshot_page_pointer(new_snapshot_id_, numa_node_, local_id);
  }
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_COMPOSER_HPP_
