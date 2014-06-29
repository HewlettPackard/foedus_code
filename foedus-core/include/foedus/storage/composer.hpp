/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_COMPOSER_HPP_
#define FOEDUS_STORAGE_COMPOSER_HPP_

#include <iosfwd>
#include <string>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/fwd.hpp"

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
 */
class Composer {
 public:
  virtual ~Composer() {}

  /** Returns a short string that briefly describes this object. */
  virtual std::string to_string() const = 0;

  /** Writes out a detailed description of this object to stream. */
  virtual void        describe(std::ostream* o) const = 0;

  virtual uint64_t    get_required_work_memory_size(
    snapshot::SortedBuffer**  log_streams,
    uint32_t                  log_streams_count) const = 0;

  virtual ErrorStack  compose(
    snapshot::SortedBuffer**          log_streams,
    uint32_t                          log_streams_count,
    const memory::AlignedMemorySlice& work_memory) = 0;

  static Composer*    create_composer(
    Engine *engine,
    const Partitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    const snapshot::Snapshot& new_snapshot);

  friend std::ostream&    operator<<(std::ostream& o, const Composer& v);
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_COMPOSER_HPP_
