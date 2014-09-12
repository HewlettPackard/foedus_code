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
#include "foedus/assorted/atomic_fences.hpp"

namespace foedus {
namespace proc {
ErrorStack ProcManagerPimpl::initialize_once() {
  // nothing to do in master engine. procedures are registered in SOC engines.
  if (engine_->is_master()) {
    return kRetOk;
  }

  LOG(INFO) << "Initializing ProcManager(" << engine_->describe_short() << ")..";
  // TODO(Hideaki) load shared libraries
  return kRetOk;
}

ErrorStack ProcManagerPimpl::uninitialize_once() {
  if (engine_->is_master()) {
    return kRetOk;
  }

  LOG(INFO) << "Uninitializing ProcManager(" << engine_->describe_short() << ")..";
  ErrorStackBatch batch;
  // TODO(Hideaki) unload shared libraries
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack  ProcManagerPimpl::pre_register(const ProcAndName& proc_and_name) {
  if (is_initialized()) {
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
ErrorStack  ProcManagerPimpl::local_register(const ProcAndName& proc_and_name) {
  if (!is_initialized()) {
    return ERROR_STACK(kErrorCodeProcRegisterTooEarly);
  }
  if (!engine_->is_master()) {
    return ERROR_STACK(kErrorCodeProcRegisterChildOnly);
  }
  LOG(INFO) << "local-registered a user procedure: " << proc_and_name.first;
  return kRetOk;
}
ErrorStack  ProcManagerPimpl::emulated_register(const ProcAndName& proc_and_name) {
  if (!is_initialized()) {
    return ERROR_STACK(kErrorCodeProcRegisterTooEarly);
  }

  EngineType soc_type = engine_->get_options().soc_.soc_type_;
  if (soc_type != kChildEmulated) {
    return ERROR_STACK(kErrorCodeProcRegisterUnsupportedSocType);
  }
  LOG(INFO) << "emulated-registered a user procedure: " << proc_and_name.first;
  return kRetOk;
}

LocalProcId ProcManagerPimpl::find_by_name(const ProcName& name, SharedData* shared_data) {
  // so far just a seqnetial search.
  LocalProcId count = shared_data->control_block_->count_;
  assorted::memory_fence_acquire();
  for (LocalProcId i = 0; i < count; ++i) {
    if (shared_data->procs_[i].first == name) {
      return i;
    }
  }
  return kLocalProcNotFound;
}

LocalProcId ProcManagerPimpl::insert(const ProcAndName& proc_and_name, SharedData* shared_data) {
  while (true) {
    bool expected = false;
    if (shared_data->control_block_->locked_.compare_exchange_weak(expected, true)) {
      break;
    }
  }
  ASSERT_ND(shared_data->control_block_->locked_.load() == true);
  LocalProcId found = find_by_name(proc_and_name.first, shared_data);
  // TODO(Hideaki) max_proc_count check.
  if (found != kLocalProcNotFound) {
    shared_data->control_block_->locked_.store(false);  // unlock
    return kLocalProcNotFound;
  }
  LocalProcId new_id = shared_data->control_block_->count_;
  shared_data->procs_[new_id] = proc_and_name;
  ++shared_data->control_block_->count_;
  // TODO(Hideaki) insert-sort to name_sort
  shared_data->control_block_->locked_.store(false);  // unlock
  return new_id;
}

}  // namespace proc
}  // namespace foedus
