/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_META_LOGGER_IMPL_HPP_
#define FOEDUS_LOG_META_LOGGER_IMPL_HPP_
#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <thread>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/log/meta_log_buffer.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

namespace foedus {
namespace log {

/**
 * @brief A log writer for metadata operation.
 * @ingroup LOG
 * @details
 * This logger instance is created only in the master engine.
 */
class MetaLogger final : public DefaultInitializable {
 public:
  explicit MetaLogger(Engine* engine) : engine_(engine), control_block_(nullptr) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  MetaLogger() = delete;
  MetaLogger(const MetaLogger &other) = delete;
  MetaLogger& operator=(const MetaLogger &other) = delete;

  friend std::ostream&    operator<<(std::ostream& o, const MetaLogger& v);

 private:
  /**
   * @brief Main routine for logger_thread_.
   * @details
   * This method keeps checking logging request in MetaLogControlBlock and writes it out
   * to the log file.
   */
  void        meta_logger_main();

  /**
   * Moves on to next file if the current file exceeds the configured max size.
   */
  ErrorStack  switch_file_if_required();

  /**
   * Adds a log entry to annotate the switch of epoch.
   * Individual log entries do not have epoch information, relying on this.
   */
  ErrorStack  log_epoch_switch(Epoch new_epoch);

  Engine* const               engine_;
  MetaLogControlBlock*        control_block_;
  std::thread                 logger_thread_;
  std::atomic<bool>           stop_requested_;
  /**
   * @brief The log file this logger is currently appending to.
   */
  fs::DirectIoFile*           current_file_;
};
static_assert(
  sizeof(MetaLogControlBlock) <= soc::GlobalMemoryAnchors::kMetaLoggerSize,
  "MetaLogControlBlock is too large.");
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_META_LOGGER_IMPL_HPP_
