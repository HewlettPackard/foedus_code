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
#include "foedus/assorted/dumb_spinlock.hpp"
#include "foedus/soc/soc_manager.hpp"

namespace foedus {
namespace proc {
ErrorStack ProcManagerPimpl::initialize_once() {
  // attach shared memories of all SOCs
  all_soc_procs_.clear();
  soc::SocId soc_count = engine_->get_options().thread_.group_count_;
  soc::SharedMemoryRepo* memory_repo = engine_->get_soc_manager().get_shared_memory_repo();
  for (soc::SocId node = 0; node < soc_count; ++node) {
    soc::NodeMemoryAnchors* anchors = memory_repo->get_node_memory_anchors(node);
    SharedData data;
    data.control_block_ = anchors->proc_manager_memory_;
    data.name_sort_ = anchors->proc_name_sort_memory_;
    data.procs_ = anchors->proc_memory_;
    all_soc_procs_.push_back(data);
  }

  if (!engine_->is_master()) {
    LOG(INFO) << "Initializing ProcManager(" << engine_->describe_short() << ")..";
    soc::SocId node = engine_->get_soc_id();
    all_soc_procs_[node].control_block_->initialize();
  }

  // TODO(Hideaki) load shared libraries
  return kRetOk;
}

ErrorStack ProcManagerPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  // TODO(Hideaki) unload shared libraries
  if (!engine_->is_master()) {
    LOG(INFO) << "Uninitializing ProcManager(" << engine_->describe_short() << ")..";
    return kRetOk;
  }
  all_soc_procs_.clear();
  return SUMMARIZE_ERROR_BATCH(batch);
}


ErrorStack  ProcManagerPimpl::get_proc(const ProcName& name, Proc* out) {
  soc::SocId node = engine_->get_soc_id();
  LocalProcId id = find_by_name(name, &all_soc_procs_[node]);
  if (id == kLocalProcNotFound) {
    return ERROR_STACK_MSG(kErrorCodeProcNotFound, name.c_str());
  }
  *out = all_soc_procs_[node].procs_[id].second;
  return kRetOk;
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
  soc::SharedMutexScope lock_scope(&shared_data->control_block_->lock_);
  LocalProcId found = find_by_name(proc_and_name.first, shared_data);
  // TODO(Hideaki) max_proc_count check.
  if (found != kLocalProcNotFound) {
    return kLocalProcNotFound;
  }
  LocalProcId new_id = shared_data->control_block_->count_;
  shared_data->procs_[new_id] = proc_and_name;
  ++shared_data->control_block_->count_;
  // TODO(Hideaki) insert-sort to name_sort
  return new_id;
}



}  // namespace proc
}  // namespace foedus
