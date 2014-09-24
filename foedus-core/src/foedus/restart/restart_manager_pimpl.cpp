/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/restart/restart_manager_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace restart {
ErrorStack RestartManagerPimpl::initialize_once() {
  if (!engine_->get_xct_manager().is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }
  control_block_ = engine_->get_soc_manager().get_shared_memory_repo()->
    get_global_memory_anchors()->restart_manager_memory_;

  // Restart manager works only in master
  if (engine_->is_master()) {
    LOG(INFO) << "Initializing RestartManager..";

    // after all other initializations, we trigger recovery procedure.
    CHECK_ERROR(recover());
  }
  return kRetOk;
}

ErrorStack RestartManagerPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  if (!engine_->get_xct_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  if (engine_->is_master()) {
    LOG(INFO) << "Uninitializing RestartManager..";
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack RestartManagerPimpl::recover() {
  Epoch durable_epoch = engine_->get_log_manager().get_durable_global_epoch();
  Epoch snapshot_epoch = engine_->get_snapshot_manager().get_snapshot_epoch();
  LOG(INFO) << "Recovering the database... durable_epoch=" << durable_epoch
    << ", snapshot_epoch=" << snapshot_epoch;

  if (durable_epoch.value() == Epoch::kEpochInitialDurable) {
    if (!snapshot_epoch.is_valid()) {
      LOG(INFO) << "The database is in initial state. Nothing to recover.";
      return kRetOk;
    } else {
      // this means durable_epoch wraps around. nothing wrong, but worth logging.
      LOG(INFO) << "Interesting. durable_epoch is initial value, but we have snapshot."
        << " This means epoch wrapped around!";
    }
  }

  if (durable_epoch == snapshot_epoch) {
    LOG(INFO) << "The snapshot is up-to-date. No need to recover.";
    return kRetOk;
  }

  return kRetOk;
}


}  // namespace restart
}  // namespace foedus
