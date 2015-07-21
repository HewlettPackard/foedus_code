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
#ifndef FOEDUS_SNAPSHOT_MAPREDUCE_BASE_IMPL_HPP_
#define FOEDUS_SNAPSHOT_MAPREDUCE_BASE_IMPL_HPP_
#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <string>
#include <thread>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/log_gleaner_ref.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief Base class for LogMapper and LogReducer to share common code.
 * @ingroup SNAPSHOT
 * @details
 * The shared parts are:
 * \li init/uninit
 * \li synchronization with gleaner (main thread)
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class MapReduceBase : public DefaultInitializable {
 public:
  MapReduceBase(Engine* engine, uint16_t id);

  MapReduceBase() = delete;
  MapReduceBase(const MapReduceBase &other) = delete;
  MapReduceBase& operator=(const MapReduceBase &other) = delete;

  LogGleanerRef*  get_parent() { return &parent_; }
  uint16_t        get_id() const { return id_; }
  uint16_t        get_numa_node() const { return numa_node_; }

  /** Expects "LogReducer-x", "LogMapper-y" etc. Used only for logging/debugging. */
  virtual std::string to_string() const = 0;

  /** Start executing */
  void          launch_thread();
  void          join_thread();

 protected:
  Engine* const                   engine_;
  LogGleanerRef                   parent_;
  /** Unique ID of this mapper or reducer. */
  const uint16_t                  id_;
  const uint16_t                  numa_node_;
  /** only for sanity check */
  std::atomic<bool>               running_;

  std::thread                     thread_;

  /** Implements the specific logics in derived class. */
  virtual ErrorStack  handle_process() = 0;

  /** Derived class's handle_process() should occasionally call this to exit if it's cancelled. */
  ErrorCode           check_cancelled() const;

 private:
  /** Main routine */
  void                handle();
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_MAPREDUCE_BASE_IMPL_HPP_
