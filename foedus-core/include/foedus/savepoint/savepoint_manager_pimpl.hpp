/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_

#include <atomic>
#include <thread>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/savepoint/fwd.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/soc/shared_cond.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"

namespace foedus {
namespace savepoint {

/** Shared data in SavepointManagerPimpl. */
struct SavepointManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  SavepointManagerControlBlock() = delete;
  ~SavepointManagerControlBlock() = delete;

  void initialize() {
    save_wakeup_.initialize();
    save_done_event_.initialize();
    savepoint_mutex_.initialize();
  }
  void uninitialize() {
    savepoint_mutex_.initialize();
    save_done_event_.uninitialize();
    save_wakeup_.uninitialize();
  }

  std::atomic<bool>   master_initialized_;
  Epoch::EpochInteger initial_current_epoch_;
  Epoch::EpochInteger initial_durable_epoch_;

  /**
   * savepoint thread sleeps on this condition variable.
   * The real variable is saved_durable_epoch_.
   */
  soc::SharedCond     save_wakeup_;
  /** The durable epoch that has been made persistent in previous savepoint-ing. */
  Epoch::EpochInteger saved_durable_epoch_;
  /**
   * Client SOC sets this value and then wakes up the savepoint thread.
   * The value indicates the epoch upto which loggers made sure all log files are durable.
   * So, as soon as we take a savepoint, this officially becomes the new global durable epoch.
   */
  Epoch::EpochInteger requested_durable_epoch_;

  /**
   * Whenever a savepoint has been taken, this event is fired.
   * The thread that has requested the savepoint sleeps on this.
   */
  soc::SharedCond     save_done_event_;

  /**
   * The content of latest savepoint.
   * This is kind of big, but most part of it is unused.
   */
  FixedSavepoint      savepoint_;
  /**
   * Read/write to savepoint_ is protected with this mutex.
   * There is anyway only one writer to savepoint_, but this is required to make sure
   * readers don't see half-updated garbage.
   */
  soc::SharedMutex    savepoint_mutex_;
};

/**
 * @brief Pimpl object of SavepointManager.
 * @ingroup SAVEPOINT
 * @details
 * A private pimpl object for SavepointManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SavepointManagerPimpl final : public DefaultInitializable {
 public:
  SavepointManagerPimpl() = delete;
  explicit SavepointManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  Epoch get_initial_current_epoch() const { return Epoch(control_block_->initial_current_epoch_); }
  Epoch get_initial_durable_epoch() const { return Epoch(control_block_->initial_durable_epoch_); }
  Epoch get_saved_durable_epoch() const { return Epoch(control_block_->saved_durable_epoch_); }
  Epoch get_requested_durable_epoch() const {
    return Epoch(control_block_->requested_durable_epoch_);
  }
  void                update_shared_savepoint(const Savepoint& src);
  LoggerSavepointInfo get_logger_savepoint(log::LoggerId logger_id);
  ErrorStack          take_savepoint(Epoch new_global_durable_epoch);

  Engine* const           engine_;
  SavepointManagerControlBlock* control_block_;


  // the followings are used only in master. child SOCs just request the master to take savepoint.
  void savepoint_main();
  bool is_stop_requested() { return savepoint_thread_stop_requested_; }

  /** The thread to take savepoints */
  std::thread             savepoint_thread_;

  std::atomic<bool>       savepoint_thread_stop_requested_;

  /** Path of the savepoint file. */
  fs::Path                savepoint_path_;

  /**
   * The current progress of the entire engine.
   */
  Savepoint               savepoint_;
};

static_assert(
  sizeof(SavepointManagerControlBlock) <= soc::GlobalMemoryAnchors::kSavepointManagerMemorySize,
  "SavepointManagerControlBlock is too large.");

}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
