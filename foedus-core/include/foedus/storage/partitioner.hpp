/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PARTITIONER_HPP_
#define FOEDUS_STORAGE_PARTITIONER_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/attachable.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
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
 * @par Partitioning Algorithm
 * So far, create_partitioner() receives only the engine object and ID to design partitioning.
 * This means we can use only information available in the status-quo of the storage in the engine,
 * such as which node owns which volatile/snapshot page.
 * This is so far enough and VERY simple/efficient to design partitioning, but later
 * we might want to explore smarter partitioning that utilizes, say, log entries.
 * (but that will be complex/expensive!)
 *
 * @par Sorting Algorithm
 * Sorting algorithm \e may use metadata specific to the storage (not just storage type).
 * For example, if we somehow know that every key in the storage is 8 byte, we can do something
 * very efficient. Or, in some case the requirement of sorting itself might depend on the storage.
 *
 * @par Shared memory, No virtual methods
 * Unfortunately, virtual methods are hard to use in multi-process and multi-node environment.
 * All dynamic data of Partitioner is stored in shared memory as explained in PartitionerMetadata.
 * Thus, Partitioner has no virtual methods. It just does switch-case on the storage type and
 * invokes methods in individual partitioners (which aren't derived classes of this).
 *
 * For more details of how individual storage types implement them, see the individual partitioners.
 */
class Partitioner CXX11_FINAL : public Attachable<PartitionerMetadata> {
 public:
  /**
  * @brief Instantiate an instance for the given storage.
  * @param[in] engine Engine
  * @param[in] id ID of the storage
  * @details
  * This method only attaches to a shared memory, so it has no cost.
  * You can instantiate Partitioner anywhere you like.
  */
  Partitioner(Engine* engine, StorageId id);

  /** Returns tiny metadata of the partitioner in shared memory. */
  const PartitionerMetadata& get_metadata() const;
  bool        is_valid()          const;
  StorageId   get_storage_id()    const { return id_;}
  StorageType get_storage_type()  const { return type_; }

  /**
   * @brief returns if this storage is partitionable.
   * @details
   * Some storage, such as a B-tree with only a single page (root=leaf), it is impossible
   * to partition. In that case, this returns false to indicate that the caller can just assume
   * all logs should be blindly sent to partition-0.
   * Similarly, if there is only one NUMA node (partition), the caller also skips partitioning,
   * but in that case the caller even skips instantiating partitioners.
   */
  bool is_partitionable();

  /**
   * @brief Determines partitioning scheme for this storage.
   * @details
   * This method puts the resulting data in shared memory.
   * This method should be called only once per snapshot.
   */
  ErrorStack design_partition();

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
  void partition_batch(
    PartitionId                     local_partition,
    const snapshot::LogBuffer&      log_buffer,
    const snapshot::BufferPosition* log_positions,
    uint32_t                        logs_count,
    PartitionId*                    results);

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
  void                sort_batch(
    const snapshot::LogBuffer&        log_buffer,
    const snapshot::BufferPosition*   log_positions,
    uint32_t                          logs_count,
    const memory::AlignedMemorySlice& sort_buffer,
    Epoch                             base_epoch,
    snapshot::BufferPosition*         output_buffer,
    uint32_t*                         written_count);

  /** Returns required size of sort buffer for sort_batch() */
  uint64_t            get_required_sort_buffer_size(uint32_t log_count);

  friend std::ostream& operator<<(std::ostream& o, const Partitioner& v);

 private:
  /** ID of the storage. */
  StorageId         id_;
  /** Type of the storage. For convenience. */
  StorageType       type_;
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
 * data_offset_ is the end of already-taken regions while data_size_ is the .
 * When we initialize a new partitioner, we lock mutex_ and increment data_offset_.
 */
struct PartitionerMetadata CXX11_FINAL {
  // This object is placed on shared memory. We only reinterpret them.
  PartitionerMetadata() CXX11_FUNC_DELETE;
  ~PartitionerMetadata() CXX11_FUNC_DELETE;

  void initialize() {
    mutex_.initialize();
    valid_ = false;
    data_offset_ = 0;
    data_size_ = 0;
  }
  void uninitialize() {
    mutex_.uninitialize();
  }

  /**
   * Serialize concurrent initialization of this partitioner.
   * This is taken only when valid_ is false or might be false.
   * As far as one observes valid_==true, he doesn't have to take mutex.
   */
  soc::SharedMutex  mutex_;
  /**
   * Whether this partitioner information (metadata+data) has been constructed.
   * When this is false, only valid_ and mutex_ can be safely accessed.
   */
  bool              valid_;
  /**
   * Relative offset from the beginning of partitioner data block that points to
   * variable-sized partitioner data.
   */
  uint32_t          data_offset_;
  /**
   * The size of the partitioner data.
   */
  uint32_t          data_size_;

  /**
   * Returns the partitioner data pointed from this metadata.
   * @pre valid_
   * @pre data_size_ > 0
   */
  void* locate_data(Engine* engine);

  /**
   * Allocates a patitioner data in shared memory of the given size.
   * @pre !valid_ (if it's already constructed, why are we allocating again?)
   * @pre locked->is_locked_by_me(), locked->get_mutex() == &mutex_ (the caller must own the mutex)
   * @pre data_size > 0
   * @post data_offset_ is set and index0-entry's data_offset_ is incremented.
   * @post data_size_ == data_size
   * The only possible error is memory running out (kErrorCodeStrPartitionerDataMemoryTooSmall).
   */
  ErrorCode allocate_data(Engine* engine, soc::SharedMutexScope* locked, uint32_t data_size);

  /**
   * Returns the shared memory for the given storage ID.
   * @pre id > 0
   */
  static PartitionerMetadata* get_metadata(Engine* engine, StorageId id);
  /** Returns the special index-0 entry that manages data block allocation for partitioners */
  static PartitionerMetadata* get_index0_metadata(Engine* engine);

  friend std::ostream& operator<<(std::ostream& o, const PartitionerMetadata& v);
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PARTITIONER_HPP_
