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
#include "foedus/log/log_id.hpp"
#include "foedus/savepoint/fwd.hpp"

namespace foedus {
namespace log {

/**
 * @brief A view of Logger object for other SOCs and master engine.
 * @ingroup LOG
 */
class LoggerRef : public Attachable<LoggerControlBlock> {
 public:
  LoggerRef();
  LoggerRef(
    Engine* engine,
    LoggerControlBlock* block,
    LoggerId id,
    uint16_t numa_node,
    uint16_t in_node_ordinal);

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

  /** Append a new epoch history. */
  void        add_epoch_history(const EpochMarkerLogType& epoch_marker);

  /**
   * @brief Constructs the range of log entries that represent the given epoch ranges.
   * @param[in] prev_epoch Log entries until this epoch are skipped.
   * An invalid epoch means from the beginning.
   * @param[in] until_epoch Log entries until this epoch are contained.
   * Must be valid.
   * @return log range that contains all logs (prev_epoch, until_epoch].
   * In other owrds, from prev_epoch-exclusive and to until_epoch-inclusive.
   * @details
   * In case there is no ending epoch marker (only when marked_epoch_ < durable_epoch_.one_more())
   * this method writes out a new epoch marker. This method is called only for each snapshotting,
   * so it shouldn't be too big a waste.
   */
  LogRange get_log_range(Epoch prev_epoch, Epoch until_epoch);

 protected:
  LoggerId  id_;
  uint16_t  numa_node_;
  uint16_t  in_node_ordinal_;
};

}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOGGER_REF_HPP_
