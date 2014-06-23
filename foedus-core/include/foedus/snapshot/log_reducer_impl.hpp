/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_REDUCER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_REDUCER_IMPL_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/mapreduce_base_impl.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief A log reducer, which receives log entries sent from mappers
 * and applies them to construct new snapshot files.
 * @ingroup SNAPSHOT
 * @details
 * @section REDUCER_OVERVIEW Overview
 * Reducers receive log entries from mappers and apply them to new snapshot files.
 *
 * @section SORTING Sorting
 * The log entries are sorted in a few steps to be processed efficiently and simply.
 *
 * @subsection STORAGE-SORT Storage Sorting
 * The first step is to sort log entries by storage.
 * We process all log entries of one storage together.
 * This has a benefit of code simplicity and less D-cache misses.
 * We don't actually sort in this case because we don't care the order between
 * storages. Thus, we use hashmap-like structure to sort based on storage-id.
 *
 * @subsection KEY-ORDINAL-SORT Key and Ordinal Sorting
 * Then, in each storage, we sort logs by keys and then by ordinal (*).
 * The algorithm to do this sorting depends on the storage type (eg Array, Masstree)
 * because some storage has a VERY efficient way to do this.
 * We exploit the fact that this sorting occurs only per storage, just passing the whole
 * log entries for the storage to storage-specific logic defined in XXX.
 * This is another reason to sort by storage first.
 *
 * (*) We do need to sort by ordinal. Otherwise correct result is not guaranteed.
 * For example, imagine the following case:
 *  \li UPDATE rec-1 to A. Log-ordinal 1.
 *  \li UPDATE rec-1 to B. Log-ordinal 2.
 * Ordinal-1 must be processed before ordinal 2.
 *
 *
 * @subsection DUMP-MERGE Dumping Logs and and Merging
 * When each reducer buffer gets full or alomost full, we do the sorting and dump it to
 * a file. When all logs are received, the reducer does merge-sort on top of the
 * sorted run files.
 *
 * @subsection COMPACTING Compacting Logs
 * In some cases, we can delete log entries for the same keys.
 * For example, when we have two logs for the same key like the example above, we can safely
 * omit the first log with ordinal 1 AS FAR AS both logs appear in the same reducer buffer
 * and updated byte positions in the record are the same.
 * Another example is updates followed by a deletion.
 *
 * This compaction is especially useful for a record that is repeatedly updated/inserted/deleted,
 * such as TPC-C's WAREHOUSE/DISTRICT records, where several thousands of overwrite-logs
 * in each reducer buffer will be compacted into just one log.
 *
 * @section DATAPAGES Data Pages
 * One tricky thing in reducer is how it manages data pages to read previous snapshot pages
 * and apply the new logs. So far, we assume each reducer allocates a sufficient amount of
 * DRAM to hold all pages it read/write during one snapshotting.
 * If this doesn't hold, we might directly allocate pages on NVRAM and read/write there.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogReducer final : public MapReduceBase {
 public:
  LogReducer(Engine* engine, LogGleaner* parent, thread::ThreadGroupId numa_node)
    : MapReduceBase(engine, parent, numa_node, numa_node) {}

  /** One LogReducer corresponds to one NUMA node (partition). */
  thread::ThreadGroupId   get_id() const { return id_; }
  std::string             to_string() const override {
    return std::string("LogReducer-") + std::to_string(id_);
  }
  friend std::ostream&    operator<<(std::ostream& o, const LogReducer& v);

 protected:
  ErrorStack  handle_initialize() override;
  ErrorStack  handle_uninitialize() override;
  ErrorStack  handle_process() override;

 private:
  /**
   * memory to store log entries in the epoch.
   * Just like the logging buffer, this forms a circular buffer.
   */
  memory::AlignedMemory   buffer_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_REDUCER_IMPL_HPP_
