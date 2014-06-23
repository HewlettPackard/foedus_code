/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PARTITIONER_HPP_
#define FOEDUS_STORAGE_PARTITIONER_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
/**
 * @brief Partitioning statistics and logic for one storage.
 * @ingroup STORAGE
 * @details
 * When the snapshot module takes snapshot, it instantiates this object for each storage
 * that had some log. Mappers obtain the object and use it to determine which partition to
 * send logs to.
 * All methods in this object are in a batched style to avoid overheads per small log entry.
 *
 * @section PARTITION_ALGO Partitioning Algorithm
 * So far, create_partitioner() receives only the engine object and ID to design partitioning.
 * This means we can use only information available in the status-quo of the storage in the engine,
 * such as which node owns which volatile/snapshot page.
 * This is so far enough and VERY simple/efficient to design partitioning, but later
 * we might want to explore smarter partitioning that utilizes, say, log entries.
 * (but that will be complex/expensive!)
 *
 * For more details of how individual storage types design partitions, see the derived classes.
 */
class Partitioner {
 public:
  /**
  * @brief Instantiate an instance for the given storage.
  * @param[in] engine Engine
  * @param[in] id ID of the storage
  * @details
  * This method not only creates an object but also designs partitioning
  * so that following calls to partition_batch() will just follow pre-deteremined
  * partitioning policy efficiently.
  */
  static Partitioner* create_partitioner(Engine* engine, StorageId id);

  virtual ~Partitioner() {}

  /**
   * @brief Clone this object, usually in order to get local copy on the same NUMA node.
   * @return Cloned object of the same type.
   * @details
   * If we have just one partitioner object for each storage, all mappers in all NUMA nodes have
   * to use the same object, most likely causing expensinve inter NUMA node communications.
   * Partitioner object is anyway small and created only once for each storage and mapper, thus
   * each mapper instead invokes this method with NumaScope to get a local copy.
   */
  virtual Partitioner* clone() const = 0;

  /**
   * @brief Identifies the partition of each log record in a batched fashion.
   * @param[in] logs pointer to log records. All of them must be logs of the storage.
   * @param[in] logs_count number of entries to process.
   * @param[out] results this method will set the partition of logs[i] to results[i].
   * @details
   * Each storage type implements this method based on the statistics passed to
   * create_partitioner(). For better performance, logs_count is usually at least thousands.
   * Assume the scale when you optimize the implementation in derived classes.
   */
  virtual ErrorStack partition_batch(
    const log::RecordLogType **logs, uint32_t logs_count, PartitionId *results) const = 0;

  /**
   * Implementation of ostream operator.
   */
  virtual void describe(std::ostream* o) const = 0;

  /** Just delegates to describe(). */
  friend std::ostream& operator<<(std::ostream& o, const Partitioner& v);
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PARTITIONER_HPP_
