/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOGGER_REF_HPP_
#define FOEDUS_LOG_LOGGER_REF_HPP_

#include "foedus/attachable.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/savepoint/fwd.hpp"

namespace foedus {
namespace log {

/**
 * @brief A view of Logger object for other SOCs and master engine.
 * @ingroup LOG
 */
class LoggerRef final : public Attachable<LoggerControlBlock> {
 public:
  LoggerRef();
  LoggerRef(Engine* engine, LoggerControlBlock* block);

  /** Returns this logger's durable epoch. */
  Epoch       get_durable_epoch() const;

  /**
   * @brief Wakes up this logger if it is sleeping.
   */
  void        wakeup();

  /**
   * @brief Wakes up this logger if its durable_epoch has not reached the given epoch yet.
   * @details
   * If this logger's durable_epoch is already same or larger than the epoch, does nothing.
   * This method just wakes up the logger and immediately returns.
   */
  void        wakeup_for_durable_epoch(Epoch desired_durable_epoch);

  /** Called from log manager's copy_logger_states. */
  void        copy_logger_state(savepoint::Savepoint *new_savepoint) const;
};

}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOGGER_REF_HPP_
