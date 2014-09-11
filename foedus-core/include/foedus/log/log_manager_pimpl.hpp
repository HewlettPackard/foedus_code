/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
#include "foedus/savepoint/fwd.hpp"
#include "foedus/thread/condition_variable_impl.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace log {
/**
 * @brief Pimpl object of LogManager.
 * @ingroup LOG
 * @details
 * A private pimpl object for LogManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class LogManagerPimpl CXX11_FINAL : public DefaultInitializable {
 public:
  /** Calculates required byte size of shared memory for this module. */
  static uint64_t get_required_shared_memory_size(const EngineOptions& options);

  LogManagerPimpl() = delete;
  explicit LogManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  void        wakeup_loggers();
  ErrorCode   wait_until_durable(Epoch commit_epoch, int64_t wait_microseconds);
  ErrorStack  refresh_global_durable_epoch();
  void        copy_logger_states(savepoint::Savepoint *new_savepoint);

  Epoch       get_durable_global_epoch() const { return Epoch(durable_global_epoch_.load()); }
  Epoch       get_durable_global_epoch_weak() const {
    return Epoch(durable_global_epoch_.load(std::memory_order_relaxed));
  }


  Engine* const               engine_;

  thread::ThreadGroupId       groups_;
  uint16_t                    loggers_per_node_;

  /**
   * Log writers.
   */
  std::vector< Logger* >      loggers_;

  /**
   * @brief The durable epoch of the entire engine.
   * @invariant current_global_epoch_ > durable_global_epoch_
   * (we need to advance current epoch to make sure the ex-current epoch is durable)
   * @details
   * This value indicates upto what commit-groups we can return results to client programs.
   * This value is advanced by checking the durable epoch of each logger.
   */
  std::atomic<Epoch::EpochInteger>    durable_global_epoch_;

  /** Fired (notify_all) whenever durable_global_epoch_ is advanced. */
  thread::ConditionVariable           durable_global_epoch_advanced_;

  /** Serializes the thread to take savepoint to advance durable_global_epoch_. */
  std::mutex                          durable_global_epoch_savepoint_mutex_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
