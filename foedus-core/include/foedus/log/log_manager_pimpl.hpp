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
#ifndef FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#define FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#include <stdint.h>

#include <atomic>
#include <vector>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/logger_ref.hpp"
#include "foedus/log/meta_log_buffer.hpp"
#include "foedus/savepoint/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/soc/shared_polling.hpp"
#include "foedus/thread/condition_variable_impl.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace log {

/** Shared data in LogManagerPimpl. */
struct LogManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  LogManagerControlBlock() = delete;
  ~LogManagerControlBlock() = delete;

  void initialize() {
    durable_global_epoch_advanced_.initialize();
    durable_global_epoch_savepoint_mutex_.initialize();
  }
  void uninitialize() {
    durable_global_epoch_savepoint_mutex_.uninitialize();
  }

  /**
   * @brief The durable epoch of the entire engine.
   * @invariant current_global_epoch_ > durable_global_epoch_
   * (we need to advance current epoch to make sure the ex-current epoch is durable)
   * @details
   * This value indicates upto what commit-groups we can return results to client programs.
   * This value is advanced by checking the durable epoch of each logger.
   */
  std::atomic<Epoch::EpochInteger>    durable_global_epoch_;

  /** Fired (broadcast) whenever durable_global_epoch_ is advanced. */
  soc::SharedPolling                  durable_global_epoch_advanced_;

  /** To-be-removed Serializes the thread to take savepoint to advance durable_global_epoch_. */
  soc::SharedMutex                    durable_global_epoch_savepoint_mutex_;
};

/**
 * @brief Pimpl object of LogManager.
 * @ingroup LOG
 * @details
 * A private pimpl object for LogManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class LogManagerPimpl final : public DefaultInitializable {
 public:
  LogManagerPimpl() = delete;
  explicit LogManagerPimpl(Engine* engine);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  void        wakeup_loggers();
  ErrorCode   wait_until_durable(Epoch commit_epoch, int64_t wait_microseconds);
  ErrorStack  refresh_global_durable_epoch();
  void        copy_logger_states(savepoint::Savepoint *new_savepoint);

  Epoch       get_durable_global_epoch() const {
    return Epoch(control_block_->durable_global_epoch_.load());
  }
  Epoch       get_durable_global_epoch_weak() const {
    return Epoch(control_block_->durable_global_epoch_.load(std::memory_order_relaxed));
  }
  void        announce_new_durable_global_epoch(Epoch new_epoch);


  Engine* const               engine_;

  thread::ThreadGroupId       groups_;
  uint16_t                    loggers_per_node_;

  /**
   * Local log writers. Index is local logger ordinal.
   * Empty in master engine.
   */
  std::vector< Logger* >      loggers_;

  /** All log writers. Index is global logger ordinal. */
  std::vector< LoggerRef >    logger_refs_;

  /** Metadata log writer. Instantiated only in master */
  MetaLogger*                 meta_logger_;
  /** Metadata log buffer. Exists in all engines */
  MetaLogBuffer               meta_buffer_;

  LogManagerControlBlock*     control_block_;
};

static_assert(
  sizeof(LogManagerControlBlock) <= soc::GlobalMemoryAnchors::kLogManagerMemorySize,
  "LogManagerControlBlock is too large.");

}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
