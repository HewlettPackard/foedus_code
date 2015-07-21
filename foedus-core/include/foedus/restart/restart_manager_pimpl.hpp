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
#ifndef FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_
#define FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/restart/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

namespace foedus {
namespace restart {
/** Shared data in RestartManagerPimpl. */
struct RestartManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  RestartManagerControlBlock() = delete;
  ~RestartManagerControlBlock() = delete;

  // nothing so far
};

/**
 * @brief Pimpl object of RestartManager.
 * @ingroup RESTART
 * @details
 * A private pimpl object for RestartManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class RestartManagerPimpl final : public DefaultInitializable {
 public:
  RestartManagerPimpl() = delete;
  explicit RestartManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  /**
   * Recover the state of database from recent snapshot files and transaction logs.
   * @pre is_initialized(), and all other modules in the engine is_initialized().
   */
  ErrorStack  recover();
  /**
   * Redo metadata operation (create/drop storage) since the latest snapshot.
   * Essentially this is the only thing the restart manager has to do.
   */
  ErrorStack  redo_meta_logs(Epoch durable_epoch, Epoch snapshot_epoch);

  Engine* const           engine_;
  RestartManagerControlBlock* control_block_;
};

static_assert(
  sizeof(RestartManagerControlBlock) <= soc::GlobalMemoryAnchors::kRestartManagerMemorySize,
  "RestartManagerControlBlock is too large.");
}  // namespace restart
}  // namespace foedus
#endif  // FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_
