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
#include <mutex>
#include <string>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
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
class LogGleaner final : public DefaultInitializable {
 public:
  explicit LogGleaner(Engine* engine, Snapshot* snapshot)
    : engine_(engine), snapshot_(snapshot) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  LogGleaner() = delete;
  LogGleaner(const LogGleaner &other) = delete;
  LogGleaner& operator=(const LogGleaner &other) = delete;

  /** Main routine of log gleaner. */
  ErrorStack execute();

  std::string             to_string() const;
  friend std::ostream&    operator<<(std::ostream& o, const LogGleaner& v);

  Snapshot*               get_snapshot() { return snapshot_; }
  LogReducer*             get_reducer(thread::ThreadGroupId partition) {
    return reducers_[partition];
  }

  bool                    is_stop_requested() const;
  void                    wakeup();

  uint16_t increment_ready_to_start_count() {
    ASSERT_ND(ready_to_start_count_ < get_all_count());
    return ++ready_to_start_count_;
  }
  uint16_t increment_completed_count() {
    ASSERT_ND(completed_count_ < get_all_count());
    return ++completed_count_;
  }
  uint16_t increment_completed_mapper_count() {
    ASSERT_ND(completed_mapper_count_ < get_mappers_count());
    return ++completed_mapper_count_;
  }
  uint16_t increment_error_count() {
    ASSERT_ND(error_count_ < get_all_count());
    return ++error_count_;
  }
  uint16_t increment_exit_count() {
    ASSERT_ND(exit_count_ < get_all_count());
    return ++exit_count_;
  }

  void clear_counts() {
    ready_to_start_count_.store(0U);
    completed_count_.store(0U);
    completed_mapper_count_.store(0U);
    error_count_.store(0U);
    exit_count_.store(0U);
    nonrecord_log_buffer_pos_.store(0U);
  }

  bool is_all_ready_to_start() const { return ready_to_start_count_ >= get_all_count(); }
  bool is_all_completed() const { return completed_count_ >= get_all_count(); }
  bool is_all_mappers_completed() const { return completed_mapper_count_ >= mappers_.size(); }
  uint16_t get_mappers_count() const { return mappers_.size(); }
  uint16_t get_reducers_count() const { return reducers_.size(); }
  uint16_t get_all_count() const { return mappers_.size() + reducers_.size(); }

  /** Called from mappers/reducers to wait until processing starts (or cancelled). */
  void wait_for_start() { start_processing_.wait(); }

  /**
   * Atomically copy the given non-record log to this gleaner's buffer, which will be centraly
   * processed at the end of epoch.
   */
  void add_nonrecord_log(const log::LogHeader* header);

  /** Returns pointers to new root pages constructed at the end of gleaning. */
  const std::map<storage::StorageId, storage::SnapshotPagePointer>& get_new_root_page_pointers()
    const {
    return new_root_page_pointers_;
  }

  /**
   * Obtains partitioner for the storage.
   */
  const storage::Partitioner* get_or_create_partitioner(storage::StorageId storage_id);
  /**
   * Returns number of partitioners created so far. After all mappers are done, this also means
   * the number of storages processed in this snapshotting.
   */
  uint32_t get_partitioner_count() const { return partitioners_.size(); }

 private:
  Engine* const                   engine_;
  Snapshot* const                 snapshot_;

  /**
   * rendezvous point after all mappers/reducers complete initialization.
   * signalled when is_all_ready_to_start() becomes true.
   */
  thread::Rendezvous              start_processing_;

  // on the other hand, mappers/reducers can wake up gleaner by accessing gleaner_thread.

  /**
   * count of mappers/reducers that are ready to start processing (finished initialization).
   * the gleaner thread is woken up when this becomes mappers_.size() + reducers_.size().
   */
  std::atomic<uint16_t>           ready_to_start_count_;

  /**
   * count of mappers/reducers that have completed processing the current epoch.
   * the gleaner thread is woken up when this becomes mappers_.size() + reducers_.size().
   * the gleaner thread sets this to zero and starts next epoch.
   */
  std::atomic<uint16_t>           completed_count_;

  /**
   * We also have a separate count for mappers only to know if all mappers are done.
   * Reducers can go into sleep only after all mappers went into sleep (otherwise reducers
   * might receive more logs!), so they have to also check this.
   */
  std::atomic<uint16_t>           completed_mapper_count_;

  /**
   * count of mappers/reducers that have exitted with some error.
   * if there happens any error, gleaner cancels all mappers/reducers.
   */
  std::atomic<uint16_t>           error_count_;
  /**
   * count of mappers/reducers that have exitted.
   * for sanity check only.
   */
  std::atomic<uint16_t>           exit_count_;

  /** Mappers. Index is LoggerId. */
  std::vector<LogMapper*>         mappers_;
  /** Reducers. Index is NUMA node ID (partition). */
  std::vector<LogReducer*>        reducers_;

  /**
   * Objects to partition log entries. Partitioners are added by mappers when they observe a
   * new Storage ID. Once added to here, a partitioner gets never changed.
   * If there is only one partition (NUMA node), this is not used.
   */
  std::map<storage::StorageId, storage::Partitioner*> partitioners_;

  /**
   * Protects read/write to partitioners_. Insertion to partitioners_ should do heavy construction
   * out of this mutex to avoid contention.
   */
  std::mutex     partitioners_mutex_;

  /**
   * Points to new root pages constructed at the end of gleaning, one for a storage.
   * This is one of the outputs the gleaner produces.
   */
  std::map<storage::StorageId, storage::SnapshotPagePointer> new_root_page_pointers_;

  /**
   * buffer to collect all logs that will be centraly processed at the end of each epoch.
   * Those are engine-targetted and storage-targetted logs, which appear much less frequently.
   * Thus this buffer is quite small.
   */
  memory::AlignedMemory           nonrecord_log_buffer_;

  /**
   * number of bytes copied into nonrecord_log_buffer_.
   * A mapper that got a non-record log atomically incrementes this value and copies into
   * nonrecord_log_buffer_ from the previous value as byte position.
   * As logs don't overlap, we don't need any mutex.
   * @see add_nonrecord_log()
   */
  std::atomic<uint64_t>           nonrecord_log_buffer_pos_;

  /**
   * Request reducers and mappers to cancel the work.
   * Blocks until all of them stop.
   */
  void cancel_reducers_mappers() {
    cancel_mappers();
    cancel_reducers();
  }
  void cancel_mappers();
  void cancel_reducers();

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
