/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_

#include <iosfwd>
#include <string>

#include "foedus/attachable.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/**
 * @brief Represents an append/scan-only store.
 * @ingroup SEQUENTIAL
 */
class SequentialStorage CXX11_FINAL : public Storage<SequentialStorageControlBlock> {
 public:
  typedef SequentialStoragePimpl   ThisPimpl;
  typedef SequentialCreateLogType  ThisCreateLogType;
  typedef SequentialMetadata       ThisMetadata;

  SequentialStorage();
  SequentialStorage(Engine* engine, SequentialStorageControlBlock* control_block);
  SequentialStorage(Engine* engine, StorageControlBlock* control_block);
  SequentialStorage(Engine* engine, StorageId id);
  SequentialStorage(Engine* engine, const StorageName& name);
  SequentialStorage(const SequentialStorage& other);
  SequentialStorage& operator=(const SequentialStorage& other);

  // Storage interface
  const SequentialMetadata*  get_sequential_metadata()  const;
  ErrorStack          create(const Metadata &metadata);
  ErrorStack          load(const StorageControlBlock& snapshot_block);
  ErrorStack          drop();

  // this storage type doesn't use moved bit

  /**
   * @brief Append one record to this sequential storage.
   * @param[in] context Thread context
   * @param[in] payload We copy from this buffer. Must be at least get_payload_size().
   * @param[in] payload_count Length of payload.
   * @pre payload_count > 0
   * @pre payload_count < kMaxPayload
   * @details
   * The strict ordering of the appended record is NOT guaranteed to be the commit serialization
   * order. \ref SEQUENTIAL storage essentially provides a set semantics, not a strictly
   * ordered LIFO queue, to be more scalable. However, the orders are at least loosely
   * ordered; the order largely represents when it was inserted.
   */
  ErrorCode  append_record(thread::Thread* context, const void *payload, uint16_t payload_count);

  /**
   * Used to apply the effect of appending to volatile list.
   */
  void       apply_append_record(thread::Thread* context, const SequentialAppendLogType* log_entry);

  // TODO(Hideaki) Scan-access methods

  friend std::ostream& operator<<(std::ostream& o, const SequentialStorage& v);
};
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_
