/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_GLEANER_IMPL_HPP_
#define FOEDUS_LOG_LOG_GLEANER_IMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <stdint.h>
#include <iosfwd>
#include <string>
namespace foedus {
namespace log {
/**
 * @brief A log-gleaner, which reads log files and scatters them to snapshot partitions.
 * @ingroup LOG
 * @details
 * @section OVERVIEW Overview
 * One log gleaner corresponds to one Logger, reading from the file the logger wrote out.
 * Log gleaners do not always exist. They are spawned when snapshotting is triggered and
 * terminated when it is done to avoid OS thread-scheduling overhead while snapshotting is not
 * taking place (which is usually the case).
 *
 * @section GLEANING Gleaning
 * The log gleaner so far simply reads from log files.
 * We have a plan to optimize its behavior when we have a large amount of DRAM by directly reading
 * from the log buffer if it is not blown away yet.
 * ThreadLogBuffer has an additional marker "head" for this purpose, but so far we don't use it
 * to simplify the implementation.
 *
 * @section SCATTERING Scattering
 * Gleaners scatter logs per epoch.
 * As log files are guaranteed to be strictly ordered by epoch (see Logger code), we can simply
 * read log files sequentially to achieve this.
 *
 * Gleaners scatter logs to partitions as follows:
 *  \li Engine-wide and Storage-wide logs (eg DROP STORAGE) are centrally processed at the end of
 * epoch. So, gleaners just buffer them while gleaning and send all of them back to the snapshot
 * thread.
 *  \li Record-wise logs always have storage-id. Gleaner checks the partitioning information for the
 * storage and sends it to corresponding buffer.
 *  \li For each epoch, the gatherd log entries are sorted by ordinal (*), then processed just like
 * usual APPLY at the end of transaction, but on top of snapshot files.
 *
 * (*) otherwise correct result is not guaranteed. For example, imagine the following case:
 *  \li UPDATE rec-1 to A. Log-ordinal 1.
 *  \li UPDATE rec-1 to B. Log-ordinal 2.
 * Ordinal-1 must be processed before ordinal 2.
 *
 * @note
 * This is a private implementation-details of \ref LOG, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogGleaner final : public DefaultInitializable {
 public:
    LogGleaner(Engine* engine, LoggerId logger_id, thread::ThreadGroupId numa_node)
        : engine_(engine), logger_id_(logger_id), numa_node_(numa_node) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    LogGleaner() = delete;
    LogGleaner(const LogGleaner &other) = delete;
    LogGleaner& operator=(const LogGleaner &other) = delete;

    void handle_gleaner();
    void request_stop() { gleaner_thread_.requst_stop(); }
    void wait_for_stop() { gleaner_thread_.wait_for_stop(); }

    std::string             to_string() const;
    friend std::ostream&    operator<<(std::ostream& o, const LogGleaner& v);

 private:
    Engine* const                   engine_;
    const LoggerId                  logger_id_;
    const thread::ThreadGroupId     numa_node_;

    thread::StoppableThread         gleaner_thread_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_GLEANER_IMPL_HPP_
