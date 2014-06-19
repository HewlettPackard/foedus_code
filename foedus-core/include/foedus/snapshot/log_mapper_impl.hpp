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
#include <foedus/memory/aligned_memory.hpp>
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
    : MapReduceBase(engine, parent, id, numa_node),
      bucket_size_kb_(0), processed_log_count_(0) {}

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
  ErrorStack  handle_process() override;
  void        pre_handle_uninitialize() override;

 private:
  /** buffer to read from file. */
  memory::AlignedMemory   io_buffer_;

  /** very small buffer to glue a log entry spanning two file reads. */
  memory::AlignedMemory   io_fragment_tmp_;

  /** memory for all partitions. Use get_bucket_slice() to get a slice of it. */
  memory::AlignedMemory   buckets_memory_;

  /** Equivalent to engine_->get_options().snapshot_.log_mapper_bucket_kb_. */
  uint32_t                bucket_size_kb_;

  /** just for reporting. */
  uint64_t                processed_log_count_;

  memory::AlignedMemorySlice get_bucket_slice(PartitionId partition) {
    uint64_t bucket_size = static_cast<uint64_t>(bucket_size_kb_) << 10;
    return memory::AlignedMemorySlice(&buckets_memory_, bucket_size * partition, bucket_size);
  }

  ErrorCode       map_log(const log::LogHeader* header);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
