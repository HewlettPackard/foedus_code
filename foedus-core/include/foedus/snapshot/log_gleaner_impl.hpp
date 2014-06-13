/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <stdint.h>
#include <iosfwd>
#include <string>
namespace foedus {
namespace snapshot {
/**
 * @brief A log-gleaner, which constructs a new set of snapshot files during snapshotting.
 * @ingroup SNAPSHOT
 * @details
 * @section OVERVIEW Overview
 * LogGleaner is the main class that manages most mechanisms to construct a new set of snapshot
 * files. Snapshot procedure constructs and calls this object during snapshot.
 * It receives partitioning policy (which snapshot partitions to send ranges of keys) per storage
 * and beginning/ending epoch of logs to \e glean while log-gleaning.
 *
 * Log-gleaning consists of two components; \b mapper (LogMapper) and \b reducer (LogReducer),
 * obviously named after the well-known map-reduce concepts.
 *
 * @section MAPPER Mapper
 * LogGleaner launches a set of mapper threads (LogMapper) to read log files.
 * Each LogMapper corresponds to foedus::log::Logger, the NUMA-local log writer which simply writes
 * out log entries produced by local worker threads.
 * Thus, the log files contain log entries that might be sent to any partitions.
 * LogMapper \e maps each log entry to some partition and send it to a reducer corresponding to the
 * partition. For more details, see LogMapper.
 *
 * @section REDUCER Reducer
 * LogGleaner also launches a set of reducer threads (LogReducer), one for each snapshot partition.
 * For each epoch, LogReducer sorts log entries sent from LogMapper.
 * The log entries are sorted by ordinal (*), then processed just like
 * usual APPLY at the end of transaction, but on top of snapshot files.
 *
 * (*) otherwise correct result is not guaranteed. For example, imagine the following case:
 *  \li UPDATE rec-1 to A. Log-ordinal 1.
 *  \li UPDATE rec-1 to B. Log-ordinal 2.
 * Ordinal-1 must be processed before ordinal 2.
 * As log entries are somewhat sorted already (due to how we write log files and buffer them in
 * mapper), we prefer bubble sort here. We so far use std::sort, though.
 *
 * @section SYNC Synchronization
 * LogGleaner coordinates the synchronization between mappers and reducers for each epoch.
 * At the beginning of each epoch, gleaner wakes up reducers and mappers. Mappers go in to sleep
 * when they process all logs in the epoch. When all mappers went to sleep, reducers start to
 * also go into sleep when they process all logs they receive.
 * When all of them are done, gleaner initiates the processing of next epoch.
 * Additionally, LogGleaner is in charge of receiving termination request from the engine
 * if the user invokes Engine::uninitialize() and requesting reducers/mappers to stop.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogGleaner final : public DefaultInitializable {
 public:
    explicit LogGleaner(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    LogGleaner() = delete;
    LogGleaner(const LogGleaner &other) = delete;
    LogGleaner& operator=(const LogGleaner &other) = delete;

    void handle_gleaner();

    /**
     * Stops all threads for this gleaning including reducers/mappers
     */
    void stop_gleaner();

    std::string             to_string() const;
    friend std::ostream&    operator<<(std::ostream& o, const LogGleaner& v);

 private:
    Engine* const                   engine_;

    thread::StoppableThread         gleaner_thread_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
