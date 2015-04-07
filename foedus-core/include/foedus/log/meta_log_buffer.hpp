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
#ifndef FOEDUS_LOG_META_LOG_BUFFER_HPP_
#define FOEDUS_LOG_META_LOG_BUFFER_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/attachable.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/soc/shared_polling.hpp"

namespace foedus {
namespace log {

/**
 * Control block for MetaLogBuffer and MetaLogger.
 */
struct MetaLogControlBlock {
  void initialize() {
    mutex_.initialize();
    logger_wakeup_.initialize();
    buffer_used_ = 0;
    oldest_offset_ = 0;
    durable_offset_ = 0;
  }
  void uninitialize() {
    mutex_.uninitialize();
  }

  bool has_waiting_log() const { return buffer_used_ > 0; }

  /**
   * The content of current log buffer. This must be the first entry to be aligned for direct-IO.
   * We put only one metadata log each time, and metadata log never gets bigger than 4kb,
   * so this is enough.
   */
  char              buffer_[1 << 12];
  uint32_t          buffer_used_;
  /** Offset from which log entries are not gleaned yet */
  uint64_t          oldest_offset_;
  /** Offset upto which log entries are fsynced */
  uint64_t          durable_offset_;
  /**
   * Accesses in MetaLogBuffer are protected with this mutex. Logger doesn't lock it.
   * Logger just checks buffer_/buffer_used_. Make sure you write buffer_ first, then buffer_used_
   * after fence.
   */
  soc::SharedMutex  mutex_;
  /** the logger sleeps on this variable */
  soc::SharedPolling  logger_wakeup_;
};

/**
 * @brief A single log buffer for metadata (eg create/drop storage).
 * @ingroup LOG
 * @details
 * Per-engine/storage operations such as CREATE/DROP STORAGE are logged differently.
 * They are always separated from usual transactions and also written to a separate log file.
 * Metadata operation is rare, so we don't optimize the code here.
 * Instead, this class is much simpler than ThreadLogBuffer. More precisely:
 *  \li Metadata operation is always the only operation in the transaction.
 *  \li Metadata operation never gets aborted.
 *  \li Every epoch has at most one metadata operation; for each metadata operation, we immediately
 * advance epoch.
 *  \li Each metadata log is immediately synched to log file.
 *
 * @par Shared log buffer
 * Unlike ThreadLogBuffer, this buffer is placed in shared memory and every thread in every SOC
 * can write to this log buffer with mutex.
 */
class MetaLogBuffer : public Attachable<MetaLogControlBlock> {
 public:
  MetaLogBuffer() : Attachable<MetaLogControlBlock>() {}
  MetaLogBuffer(Engine* engine, MetaLogControlBlock* block)
    : Attachable<MetaLogControlBlock>(engine, block) {}

  /**
   * Synchronously writes out the given log to metadata log file.
   */
  void commit(BaseLogType* metalog, Epoch* commit_epoch);

  friend std::ostream& operator<<(std::ostream& o, const MetaLogBuffer& v);
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_META_LOG_BUFFER_HPP_

