/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PARTITIONER_HPP_
#define FOEDUS_STORAGE_PARTITIONER_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/log_buffer.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
/**
 * @brief Partitioning and sorting logic for one storage.
 * @ingroup STORAGE
 * @details
 * When the snapshot module takes snapshot, it instantiates this object for each storage
 * that had some log. Mappers obtain the object and use it to determine which partition to
 * send logs to. Reducers also use it to sort individual logs in the storage by key-then-ordinal
 * order.
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
 * @section SORT_ALGO Sorting Algorithm
 * Sorting algorithm \e may use metadata specific to the storage (not just storage type).
 * For example, if we somehow know that every key in the storage is 8 byte, we can do something
 * very efficient. Or, in some case the requirement of sorting itself might depend on the storage.
 *
 * For more details of how individual storage types implement them, see the derived classes.
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

  virtual StorageId get_storage_id() const = 0;
  virtual StorageType get_storage_type() const = 0;

  /**
   * @brief Clone this object, usually in order to get local copy on the same NUMA node.
   * @return Cloned object of the same type.
   * @details
   * If we have just one partitioner object for each storage, all mappers in all NUMA nodes have
   * to use the same object, most likely causing expensinve inter NUMA node communications.
   * Partitioner object is anyway small and created only once for each storage and mapper, thus
   * each mapper instead invokes this method within NumaScope to get a local copy.
   */
  virtual Partitioner* clone() const = 0;

  /**
   * @brief returns if this storage is partitionable.
   * @details
   * Some storage, such as a B-tree with only a single page (root=leaf), it is impossible
   * to partition. In that case, this returns false to indicate that the caller can just assume
   * all logs should be blindly sent to partition-0.
   * Similarly, if there is only one NUMA node (partition), the caller also skips partitioning,
   * but in that case the caller even skips instantiating partitioners.
   */
  virtual bool is_partitionable() const = 0;

  /**
   * @brief Identifies the partition of each log record in a batched fashion.
   * @param[in] local_partition The node the caller (mapper) resides in.
   * @param[in] log_buffer Converts from positions to physical pointers.
   * @param[in] log_positions positions of log records. All of them must be logs of this storage.
   * @param[in] logs_count number of entries to process.
   * @param[out] results this method will set the partition of logs[i] to results[i].
   * @pre !is_partitionable(): in this case, it's waste of time. check it before calling this.
   * @details
   * Each storage type implements this method based on the statistics passed to
   * create_partitioner(). For better performance, logs_count is usually at least thousands.
   * Assume the scale when you optimize the implementation in derived classes.
   */
  virtual void partition_batch(
    PartitionId                     local_partition,
    const snapshot::LogBuffer&      log_buffer,
    const snapshot::BufferPosition* log_positions,
    uint32_t                        logs_count,
    PartitionId*                    results) const = 0;

  /**
   * @brief Called from log reducer to sort log entries by keys.
   * @param[in] log_buffer Converts from positions to physical pointers.
   * @param[in] log_positions positions of log records. All of them must be logs of this storage.
   * @param[in] logs_count number of entries to process.
   * @param[in] sort_buffer For whatever purpose, the implementation can use this buffer as
   * temporary working space.
   * @param[in] base_epoch All log entries in this inputs are assured to be after this epoch.
   * Also, it is assured to be within 2^16 from this epoch.
   * Even with 10 milliseconds per epoch, this corresponds to more than 10 hours.
   * Snapshot surely happens more often than that.
   * @param[out] output_buffer sorted results are written to this variable.
   * the buffer size is at least of log_positions_count_.
   * @param[out] written_count how many logs written to output_buffer. If there was no compaction,
   * this will be same as log_positions_count_.
   * @details
   * All log entries passed to this method are for this storage.
   * Each storage type implements an efficient and batched way of sorting all log entries
   * by key-and-then-ordinal.
   * The implementation can do \b compaction when it is safe.
   * For example, two \e ovewrite logs on the same key's same data region can be compacted to
   * one log. In that case, written_count becomes smaller than log_positions_count_.
   * @see get_required_sort_buffer_size()
   */
  virtual void                sort_batch(
    const snapshot::LogBuffer&        log_buffer,
    const snapshot::BufferPosition*   log_positions,
    uint32_t                          logs_count,
    const memory::AlignedMemorySlice& sort_buffer,
    Epoch                             base_epoch,
    snapshot::BufferPosition*         output_buffer,
    uint32_t*                         written_count) const = 0;

  /** Returns required size of sort buffer for sort_batch() */
  virtual uint64_t            get_required_sort_buffer_size(uint32_t log_count) const = 0;

  /**
   * Implementation of ostream operator.
   */
  virtual void describe(std::ostream* o) const = 0;

  /** Just delegates to describe(). */
  friend std::ostream& operator<<(std::ostream& o, const Partitioner& v);
};

/**
 * @brief Tiny metadata of partitioner for every storage used while log gleaning.
 * @ingroup STORAGE
 * @details
 * The metadata is tiny because it just points to a data block in a separate partitioner data block,
 * which is variable-sized. Think of Masstree's partitioning information for example. we have to
 * store keys, so we can't statically determine the size.
 * We allocate an array of this object on shared memory so that all mappers/reducers can
 * access the partitioner information.
 *
 * @par Index-0 Entry
 * As storage-id 0 doesn't exist, we use the first entry as metadata of the data block.
 * data_size_ is the end of already-taken regions. When we initialize a new partitioner,
 * we lock initialize_mutex_ and increment data_size_.
 */
struct PartitionerMetadata {
  // This object is placed on shared memory. We only reinterpret them.
  PartitionerMetadata() CXX11_FUNC_DELETE;
  ~PartitionerMetadata() CXX11_FUNC_DELETE;

  /**
   * Serialize concurrent initialization of this partitioner.
   * This is taken only when initialized_ is false or might be false.
   * As far as one observes initialized_==true, he doesn't have to take mutex.
   */
  soc::SharedMutex  initialize_mutex_;
  /**
   * Whether this partitioner information (metadata+data) has been initialized.
   * When this is false, only initialized_ and initialize_mutex_ can be safely accessed.
   */
  bool              initialized_;
  /** Type of the storage. For convenience. */
  StorageType       type_;
  /** ID of the storage. For convenience. */
  StorageId         id_;
  /**
   * Relative offset from the beginning of partitioner data block that points to
   * variable-sized partitioner data.
   */
  uint32_t          data_offset_;
  /**
   * The size of the partitioner data.
   */
  uint32_t          data_size_;
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PARTITIONER_HPP_
