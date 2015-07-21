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
#include "foedus/assert_nd.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_manager_pimpl.hpp"
namespace foedus {
namespace log {
LogManager::LogManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new LogManagerPimpl(engine);
}
LogManager::~LogManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  LogManager::initialize() { return pimpl_->initialize(); }
bool        LogManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  LogManager::uninitialize() { return pimpl_->uninitialize(); }

void        LogManager::wakeup_loggers() { pimpl_->wakeup_loggers(); }
Epoch       LogManager::get_durable_global_epoch() const {
  return pimpl_->get_durable_global_epoch();
}
Epoch       LogManager::get_durable_global_epoch_weak() const {
  return pimpl_->get_durable_global_epoch_weak();
}
void LogManager::announce_new_durable_global_epoch(Epoch new_epoch) {
  pimpl_->announce_new_durable_global_epoch(new_epoch);
}

ErrorCode   LogManager::wait_until_durable(Epoch commit_epoch, int64_t wait_microseconds) {
  return pimpl_->wait_until_durable(commit_epoch, wait_microseconds);
}
LoggerRef   LogManager::get_logger(LoggerId logger_id) {
  ASSERT_ND(logger_id < pimpl_->logger_refs_.size());
  return pimpl_->logger_refs_[logger_id];
}

ErrorStack LogManager::refresh_global_durable_epoch() {
  return pimpl_->refresh_global_durable_epoch();
}
void LogManager::copy_logger_states(savepoint::Savepoint* new_savepoint) {
  pimpl_->copy_logger_states(new_savepoint);
}

MetaLogBuffer* LogManager::get_meta_buffer() {
  return &pimpl_->meta_buffer_;
}

MetaLogger* LogManager::get_meta_logger() {
  return pimpl_->meta_logger_;
}

}  // namespace log
}  // namespace foedus
