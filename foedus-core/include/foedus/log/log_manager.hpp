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
#ifndef FOEDUS_LOG_LOG_MANAGER_HPP_
#define FOEDUS_LOG_LOG_MANAGER_HPP_
#include <stdint.h>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/log/logger_ref.hpp"
#include "foedus/savepoint/fwd.hpp"

namespace foedus {
namespace log {
/**
 * @brief Log Manager class that provides API to write/read transaction logs.
 * @ingroup LOG
 */
class LogManager CXX11_FINAL : public virtual Initializable {
 public:
  explicit LogManager(Engine* engine);
  ~LogManager();

  // Disable default constructors
  LogManager() CXX11_FUNC_DELETE;
  LogManager(const LogManager&) CXX11_FUNC_DELETE;
  LogManager& operator=(const LogManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /**
   * @brief Fillup the given savepoint with the current information of the loggers.
   * @details
   * This is called as a part of taking a savepoint.
   */
  void        copy_logger_states(savepoint::Savepoint *new_savepoint);

  /**
   * @brief Wake up loggers if they are sleeping.
   * @details
   * This method might be called from several places for various events to make sure
   * loggers catch up.
   * As far as you don't call this method too often, like tens of thousands times per second,
   * it wouldn't hurt.
   */
  void        wakeup_loggers();

  /**
   * Returns a reference to the logger of the given ID.
   */
  LoggerRef   get_logger(LoggerId logger_id);

  /**
   * @brief Returns the durable epoch of the entire engine.
   * @invariant current_global_epoch > durable_global_epoch
   * (we need to advance current epoch to make sure the ex-current epoch is durable)
   * @details
   * This value indicates upto what commit-groups we can return results to client programs.
   * This value is advanced by checking the durable epoch of each logger.
   */
  Epoch       get_durable_global_epoch() const;
  /** Non-atomic version of the method. */
  Epoch       get_durable_global_epoch_weak() const;
  /**
   * Sets the new global durable epoch and also wakes up threads that were waiting for it.
   */
  void        announce_new_durable_global_epoch(Epoch new_epoch);

  /**
   * @brief Synchronously blocks until the durable global epoch reaches the given commit
   * epoch or the given duration elapses.
   * @param[in] commit_epoch Returns kRetOk \e iff the durable global epoch reaches this value.
   * @param[in] wait_microseconds Or, returns a TIMEOUT error when this duration elapses,
   * whichever comes first. Negative value means waiting forever. 0 means \e conditional,
   * immediately returning without blocking, which is useful to quickly check the committed-ness.
   * @details
   * Client programs can either call this method for each transaction right after precommit_xct()
   * or call this method after a bunch of precommit_xct() calls (\e group-commit).
   * In either case, remember that \b both read-only and read-write transactions must not return
   * results to clients until the durable global epoch reaches the given commit epoch.
   * Otherwise, you violate serializability (which might be okay depending on your desired
   * isolation level).
   */
  ErrorCode   wait_until_durable(Epoch commit_epoch, int64_t wait_microseconds = -1);

  /**
   * @brief Called whenever there is a chance that the global durable epoch advances.
   * @details
   * Each logger calls this method when they advance their local durable epoch, which may
   * or may not advance the global durable epoch. This method recalculates global durable epoch.
   */
  ErrorStack  refresh_global_durable_epoch();

  MetaLogger*     get_meta_logger();
  MetaLogBuffer*  get_meta_buffer();

 private:
  LogManagerPimpl *pimpl_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_MANAGER_HPP_
