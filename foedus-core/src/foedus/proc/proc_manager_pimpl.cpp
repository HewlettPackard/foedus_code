/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/proc/proc_manager_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"

namespace foedus {
namespace proc {
ErrorStack ProcManagerPimpl::initialize_once() {
  LOG(INFO) << "Initializing ProcManager..";

  // TODO(Hideaki) load shared libraries
  return kRetOk;
}

ErrorStack ProcManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing ProcManager..";
  ErrorStackBatch batch;
  // TODO(Hideaki) unload shared libraries
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack  ProcManagerPimpl::pre_register(ProcAndName proc_and_name) {
  if (engine_->is_initialized()) {
    return ERROR_STACK(kErrorCodeProcPreRegisterTooLate);
  }
  if (!engine_->is_master()) {
    return ERROR_STACK(kErrorCodeProcRegisterMasterOnly);
  }
  EngineType soc_type = engine_->get_options().soc_.soc_type_;
  if (soc_type != kChildEmulated && soc_type != kChildForked) {
    return ERROR_STACK(kErrorCodeProcRegisterUnsupportedSocType);
  }
  pre_registered_procs_.push_back(proc_and_name);
  LOG(INFO) << "pre-registered a user procedure: " << proc_and_name.first;
  return kRetOk;
}
ErrorStack  ProcManagerPimpl::local_register(ProcAndName proc_and_name) {
  if (!engine_->is_initialized()) {
    return ERROR_STACK(kErrorCodeProcRegisterTooEarly);
  }
  if (!engine_->is_master()) {
    return ERROR_STACK(kErrorCodeProcRegisterChildOnly);
  }
  LOG(INFO) << "local-registered a user procedure: " << proc_and_name.first;
  return kRetOk;
}
ErrorStack  ProcManagerPimpl::emulated_register(ProcAndName proc_and_name) {
  if (!engine_->is_initialized()) {
    return ERROR_STACK(kErrorCodeProcRegisterTooEarly);
  }

  EngineType soc_type = engine_->get_options().soc_.soc_type_;
  if (soc_type != kChildEmulated) {
    return ERROR_STACK(kErrorCodeProcRegisterUnsupportedSocType);
  }
  LOG(INFO) << "emulated-registered a user procedure: " << proc_and_name.first;
  return kRetOk;
}

}  // namespace proc
}  // namespace foedus
