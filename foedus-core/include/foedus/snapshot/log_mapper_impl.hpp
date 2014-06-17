/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/snapshot/mapreduce_base_impl.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <stdint.h>
#include <iosfwd>
#include <string>
namespace foedus {
namespace snapshot {
/**
 * @brief A log mapper, which reads log files from one logger and
 * sends them to corresponding log reducers.
 * @ingroup SNAPSHOT
 * @details
 * @section MAPPER_OVERVIEW Overview
 * Mappers read logs per epoch.
 * As log files are guaranteed to be strictly ordered by epoch (see Logger code), we can simply
 * read log files sequentially to achieve this.
 *
 * Mappers send logs to partitions as follows:
 *  \li Engine-wide and Storage-wide logs (eg DROP STORAGE) are centrally processed at the end of
 * epoch. So, mappers just buffer them and send all of them back to LogGleaner, which will process
 * all of them.
 *  \li Record-wise logs always have storage-id. Mappers check the partitioning information for the
 * storage and send it to corresponding reducers (with buffering to avoid per-log communication).
 *
 * @section MAPPER_OPTIMIZATION Possible Optimization
 * The log gleaner so far simply reads from log files.
 * We have a plan to optimize its behavior when we have a large amount of DRAM by directly reading
 * from the log buffer if it is not blown away yet.
 * ThreadLogBuffer has an additional marker "head" for this purpose, but so far we don't use it
 * to simplify the implementation.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogMapper final : public MapReduceBase {
 public:
    LogMapper(Engine* engine, LogGleaner* parent, log::LoggerId id, thread::ThreadGroupId numa_node)
        : MapReduceBase(engine, parent, id, numa_node) {}

    /**
     * Unique ID of this log mapper. One log mapper corresponds to one logger, so this ID is also
     * the corresponding logger's ID (log::LoggerId).
     */
    log::LoggerId           get_id() const { return id_; }
    std::string             to_string() const override {
        return std::string("LogMapper-") + std::to_string(id_);
    }
    friend std::ostream&    operator<<(std::ostream& o, const LogMapper& v);

 protected:
    ErrorStack  handle_initialize() override;
    ErrorStack  handle_uninitialize() override;
    ErrorStack  handle_epoch() override;
    void        pre_wait_for_next_epoch() override;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
