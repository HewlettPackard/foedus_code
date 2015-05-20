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
#ifndef FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
#define FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"
namespace foedus {
namespace storage {
/**
 * @brief Set of options for storage manager.
 * @ingroup STORAGE
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct StorageOptions CXX11_FINAL : public virtual externalize::Externalizable {
  enum Constants {
    kDefaultMaxStorages = 1 << 9,
    kDefaultPartitionerDataMemoryMb = 1,
  };
  /**
   * Constructs option values with default values.
   */
  StorageOptions();

  /**
   * Maximum number of storages in this database.
   */
  uint32_t                max_storages_;

  /**
   * Size in MB of a shared memory buffer allocated for all partitioners during log gleaning.
   * Increase this value when you have a large number of storages that have large partitioning
   * information (eg. long keys).
   * So far, this must be less than 4GB.
   */
  uint32_t                partitioner_data_memory_mb_;

  EXTERNALIZABLE(StorageOptions);
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
