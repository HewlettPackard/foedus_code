/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_HPP_
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/savepoint/fwd.hpp"
namespace foedus {
namespace savepoint {
/**
 * @brief Savepoint manager that atomically and durably writes out a savepoint file.
 * @ingroup SAVEPOINT
 */
class SavepointManager CXX11_FINAL : public virtual Initializable {
 public:
  explicit SavepointManager(Engine* engine);
  ~SavepointManager();

  // Disable default constructors
  SavepointManager() CXX11_FUNC_DELETE;
  SavepointManager(const SavepointManager&) CXX11_FUNC_DELETE;
  SavepointManager& operator=(const SavepointManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /**
   * @brief Returns the saved information of the given logger in latest savepoint.
   * @details
   * Note that this is a read-only access, which might see a stale information if it's
   * in race condition. However, we take a lock before copying the entire information.
   * Thus, this method is slow but safe. No garbage information returned.
   */
  LoggerSavepointInfo       get_logger_savepoint(log::LoggerId logger_id);

  /** Returns the saved information of metadata logger in lateset savepoint */
  void get_meta_logger_offsets(uint64_t* oldest_offset, uint64_t* durable_offset) const;

  Epoch get_initial_current_epoch() const;
  Epoch get_initial_durable_epoch() const;
  Epoch get_saved_durable_epoch() const;

  /**
   * @brief Atomically and durably takes a savepoint for the given epoch advancement.
   * @details
   * This is called from log manager when it sees all loggers flushed their logs up to
   * the given epoch \b BEFORE the log manager announces the new global durable epoch to others.
   * This is the last step in the system to adavance a global durable epoch, thus officially
   * committing transactions in the epoch. Until this method completes, the transactions are
   * not yet committed.
   */
  ErrorStack      take_savepoint(Epoch new_global_durable_epoch);

 private:
  SavepointManagerPimpl *pimpl_;
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_HPP_
