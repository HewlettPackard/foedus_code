/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
#include <foedus/assert_nd.hpp>
#include <foedus/epoch.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/thread/condition_variable_impl.hpp>
#include <foedus/thread/fwd.hpp>
#include <stdint.h>
#include <atomic>
#include <iosfwd>
#include <mutex>
#include <string>
#include <vector>
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
 * @section MAPPER Mapper
 * LogGleaner launches a set of mapper threads (foedus::snapshot::LogMapper) to read log files.
 * Each LogMapper corresponds to foedus::log::Logger, the NUMA-local log writer which simply writes
 * out log entries produced by local worker threads.
 * Thus, the log files contain log entries that might be sent to any partitions.
 * LogMapper \e maps each log entry to some partition and send it to a reducer corresponding to the
 * partition. For more details, see foedus::snapshot::LogMapper.
 *
 * @section REDUCER Reducer
 * LogGleaner also launches a set of reducer threads (foedus::snapshot::LogReducer), one for each
 * snapshot partition. For each epoch, LogReducer sorts log entries sent from LogMapper.
 * The log entries are sorted by ordinal (*), then processed just like
 * usual APPLY at the end of transaction, but on top of snapshot files.
 *
 * (*) otherwise correct result is not guaranteed. For example, imagine the following case:
 *  \li UPDATE rec-1 to A. Log-ordinal 1.
 *  \li UPDATE rec-1 to B. Log-ordinal 2.
 *
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
 * Reducers/mappers check if they are requested to stop when they get idle or complete all work
 * in an epoch. Thus, even in the worst case it stops its work after processing logs of one-epoch,
 * which shouldn't be catastrophic because one epoch is only tens of milliseconds.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogGleaner final : public DefaultInitializable {
 public:
    explicit LogGleaner(Engine* engine, Snapshot* snapshot, thread::StoppableThread* gleaner_thread)
        : engine_(engine), snapshot_(snapshot), gleaner_thread_(gleaner_thread) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    LogGleaner() = delete;
    LogGleaner(const LogGleaner &other) = delete;
    LogGleaner& operator=(const LogGleaner &other) = delete;

    /** Main routine of log gleaner. */
    ErrorStack execute();

    std::string             to_string() const;
    friend std::ostream&    operator<<(std::ostream& o, const LogGleaner& v);


    Epoch                   get_processing_epoch() const { return Epoch(processing_epoch_.load()); }
    Epoch                   get_next_processing_epoch() const {
        Epoch cur_epoch = get_processing_epoch();
        // if there was no snapshot (initial snapshot), then the first epoch that might have logs
        // is kEpochInitialCurrent.
        return cur_epoch.is_valid() ? cur_epoch.one_more() : Epoch(Epoch::kEpochInitialCurrent);
    }

    thread::ConditionVariable&      processing_epoch_cond_for(Epoch epoch) {
        if (epoch.value() & 1) {
            return processing_epoch_cond_[1];
        } else {
            return processing_epoch_cond_[0];
        }
    }

    Snapshot*               get_snapshot() { return snapshot_; }

    bool                    is_stop_requested() const;
    void                    wakeup();

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

    bool is_all_completed() const { return completed_count_ >= get_all_count(); }
    bool is_all_mappers_completed() const { return completed_mapper_count_ >= mappers_.size(); }
    uint16_t get_mappers_count() const { return mappers_.size(); }
    uint16_t get_reducers_count() const { return reducers_.size(); }
    uint16_t get_all_count() const { return mappers_.size() + reducers_.size(); }

    /**
     * Atomically copy the given non-record log to this gleaner's buffer, which will be centraly
     * processed at the end of epoch.
     */
    void add_nonrecord_log(const log::LogHeader* header);

 private:
    /**
     * Request reducers and mappers to cancel the work.
     * Blocks until all of them stop.
     */
    void cancel_reducers_mappers();

    Engine* const                   engine_;
    Snapshot* const                 snapshot_;

    /**
     * The thread that will call execute(). execute() occasionally checks
     * if this thread has been requested to stop, and exit if that happens.
     */
    thread::StoppableThread* const  gleaner_thread_;

    /**
     * @brief Used to wake up mappers/reducers by gleaner.
     * @details
     * Gleaner has two of them to separate 1) mappers/reducers that are still waiting to be woken up
     * after finishing the previous epoch from 2) those that are already woken up and even finished
     * the current epoch.
     * [0] is used when they are waiting for even epoch, [1] for odd epoch.
     * Kind of stupid. only if std c++ employed boost barriers... (again, we don't use boost).
     */
    thread::ConditionVariable       processing_epoch_cond_[2];

    // on the other hand, mappers/reducers can wake up gleaner by accessing gleaner_thread.

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

    /**
     * The epoch mappers/reducers are supposed to be processing now.
     * When the processing is not started yet (mappers/reducers initialization phase),
     * the value is snapshot_->base_epoch_.
     * When all processing is done, snapshot_->valid_until_epoch_.one_more().
     */
    std::atomic<Epoch::EpochInteger>    processing_epoch_;

    /** Mappers. Index is LoggerId. */
    std::vector<LogMapper*>         mappers_;
    /** Reducers. Index is PartitionId. */
    std::vector<LogReducer*>        reducers_;

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
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_GLEANER_IMPL_HPP_
