/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <map>
#include <string>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/attachable.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/log_gleaner_ref.hpp"
#include "foedus/snapshot/snapshot_manager_pimpl.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/rendezvous_impl.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief A log-gleaner, which constructs a new set of snapshot files during snapshotting.
 * @ingroup SNAPSHOT
 * @details
 * @section GLEANER_OVERVIEW Log Gleaner Overview
 * LogGleaner is the main class that manages most mechanisms to construct a new set of snapshot
 * files. Snapshot procedure constructs and calls this object during snapshot.
 * It receives partitioning policy (which snapshot partitions to send ranges of keys) per storage
 * and beginning/ending epoch of logs to \e glean while log-gleaning.
 *
 * Log-gleaning consists of two components; \b mapper (foedus::snapshot::LogMapper) and \b reducer
 * (foedus::snapshot::LogReducer), obviously named after the well-known map-reduce concepts.
 *
 * @section GLEANER_MAPPER Mapper
 * LogGleaner launches a set of mapper threads (foedus::snapshot::LogMapper) to read log files.
 * Each LogMapper corresponds to foedus::log::Logger, the NUMA-local log writer which simply writes
 * out log entries produced by local worker threads.
 * Thus, the log files contain log entries that might be sent to any partitions.
 * LogMapper \e maps each log entry to some partition and send it to a reducer corresponding to the
 * partition. For more details, see foedus::snapshot::LogMapper.
 *
 * @section GLEANER_REDUCER Reducer
 * LogGleaner also launches a set of reducer threads (foedus::snapshot::LogReducer), one for each
 * NUMA node. LogReducer sorts log entries sent from LogMapper.
 * The log entries are sorted by key and ordinal (*), then processed just like
 * usual APPLY at the end of transaction, but on top of snapshot files.
 *
 * (*) otherwise correct result is not guaranteed. For example, imagine the following case:
 *  \li UPDATE rec-1 to A. Log-ordinal 1.
 *  \li UPDATE rec-1 to B. Log-ordinal 2.
 *
 * Ordinal-1 must be processed before ordinal 2.
 *
 * @section GLEANER_SYNC Synchronization
 * LogGleaner coordinates the synchronization between mappers and reducers during snapshotting.
 * At the beginning of snapshotting, gleaner wakes up reducers and mappers. Mappers go in to sleep
 * when they process all logs. When all mappers went to sleep, reducers start to
 * also go into sleep when they process all logs they receive.
 * When all of them are done, gleaner initiates the last wrap-up phase.
 * Additionally, LogGleaner is in charge of receiving termination request from the engine
 * if the user invokes Engine::uninitialize() and requesting reducers/mappers to stop.
 *
 * Reducers/mappers occasionally check if they are requested to stop when they get idle or complete
 * all work. They do the check at least once for a while so that the latency to stop can not be
 * catastrophic.
 *
 * @section GLEANER_ROOT Constructing Root Pages
 * After all mappers and reducers complete, the last phase of log gleaning is to construct
 * root pages for the storages modified in this snapshotting.
 * Gleaner collects \e root-page-info from each reducer and combines them to create the
 * root page(s). When all set, gleaner produces maps from storage ID to a new root page ID.
 * This will be written out in a snapshot metadata file by snapshot manager.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogGleaner final : public LogGleanerRef {
 public:
  LogGleaner(Engine* engine, const Snapshot& new_snapshot);

  LogGleaner() = delete;
  LogGleaner(const LogGleaner &other) = delete;
  LogGleaner& operator=(const LogGleaner &other) = delete;

  /** Main routine of log gleaner. */
  ErrorStack execute();

  std::string             to_string() const;
  friend std::ostream&    operator<<(std::ostream& o, const LogGleaner& v);

  /** Returns pointers to new root pages constructed at the end of gleaning. */
  const std::map<storage::StorageId, storage::SnapshotPagePointer>& get_new_root_page_pointers()
    const {
    return new_root_page_pointers_;
  }

 private:
  /** The snapshot we are now taking. */
  const Snapshot                  new_snapshot_;

  /**
   * Points to new root pages constructed at the end of gleaning, one for a storage.
   * This is one of the outputs the gleaner produces.
   */
  std::map<storage::StorageId, storage::SnapshotPagePointer> new_root_page_pointers_;

  /** Before starting log gleaner, this method resets all shared memory to initialized state. */
  void      clear_all();

  /**
   * As the first step, this method investigates existing storages and determines the partitioning
   * policy for them.
   */
  ErrorStack design_partitions();
  /**
   * design_partitions() invokes this to parallelize the partitioning.
   */
  void design_partitions_run(storage::StorageId from, storage::StorageId count, ErrorStack* result);

  /**
   * Request reducers and mappers to cancel the work.
   * Blocks until all of them stop (or timeout).
   */
  ErrorStack cancel_reducers_mappers();

  /**
   * @brief Final sub-routine of execute()
   * @details
   * Collects what each reducer wrote and combines them to be new root page(s) for each storage.
   * This method fills out new_root_page_pointers_ as the result.
   */
  ErrorStack construct_root_pages();
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
