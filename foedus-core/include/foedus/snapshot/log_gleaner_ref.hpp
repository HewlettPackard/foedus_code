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
#ifndef FOEDUS_SNAPSHOT_LOG_GLEANER_REF_HPP_
#define FOEDUS_SNAPSHOT_LOG_GLEANER_REF_HPP_

#include <stdint.h>

#include "foedus/attachable.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/fwd.hpp"

namespace foedus {
namespace snapshot {
/**
 * A remote view of LogGleaner from all engines.
 * @ingroup SNAPSHOT
 */
class LogGleanerRef : public Attachable<LogGleanerControlBlock> {
 public:
  LogGleanerRef();
  explicit LogGleanerRef(Engine* engine);

  bool        is_error() const;
  void        wakeup();

  const Snapshot& get_cur_snapshot() const;
  SnapshotId  get_snapshot_id() const;
  Epoch       get_base_epoch() const;
  Epoch       get_valid_until_epoch() const;

  uint16_t increment_completed_count();
  uint16_t increment_completed_mapper_count();
  uint16_t increment_error_count();
  uint16_t increment_exit_count();

  bool is_all_exitted() const;
  bool is_all_completed() const;
  bool is_all_mappers_completed() const;
  uint16_t get_mappers_count() const;
  uint16_t get_reducers_count() const;
  uint16_t get_all_count() const;

 protected:
  storage::PartitionerMetadata* partitioner_metadata_;
  void*                         partitioner_data_;
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_GLEANER_REF_HPP_
