/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/proc/proc_manager_pimpl.hpp"

#include <glog/logging.h>

#include <sstream>
#include <string>

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
    get_local_data()->control_block_->initialize();
  }

  // TODO(Hideaki) load shared libraries
  return kRetOk;
}

ErrorStack ProcManagerPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  // TODO(Hideaki) unload shared libraries
  if (!engine_->is_master()) {
    LOG(INFO) << "Uninitializing ProcManager(" << engine_->describe_short() << ")..";
    get_local_data()->control_block_->uninitialize();
    return kRetOk;
  }
  all_soc_procs_.clear();
  return SUMMARIZE_ERROR_BATCH(batch);
}


ErrorStack  ProcManagerPimpl::get_proc(const ProcName& name, Proc* out) {
  soc::SocId node = engine_->get_soc_id();
  LocalProcId id = find_by_name(name, &all_soc_procs_[node]);
  if (id == kLocalProcInvalid) {
    return ERROR_STACK_MSG(kErrorCodeProcNotFound, name.c_str());
  }
  *out = all_soc_procs_[node].procs_[id].second;
  return kRetOk;
}
ProcManagerPimpl::SharedData* ProcManagerPimpl::get_local_data() {
  ASSERT_ND(!engine_->is_master());
  return &all_soc_procs_[engine_->get_soc_id()];
}
const ProcManagerPimpl::SharedData* ProcManagerPimpl::get_local_data() const {
  ASSERT_ND(!engine_->is_master());
  return &all_soc_procs_[engine_->get_soc_id()];
}

ErrorStack  ProcManagerPimpl::pre_register(const ProcAndName& proc_and_name) {
  if (is_initialized()) {
    LOG(ERROR) << "Incorrect use of pre_register(): "
      << get_error_message(kErrorCodeProcPreRegisterTooLate);
    return ERROR_STACK(kErrorCodeProcPreRegisterTooLate);
  }
  if (!engine_->is_master()) {
    LOG(ERROR) << "Incorrect use of pre_register(): "
      << get_error_message(kErrorCodeProcRegisterMasterOnly);
    return ERROR_STACK(kErrorCodeProcRegisterMasterOnly);
  }
  EngineType soc_type = engine_->get_options().soc_.soc_type_;
  if (soc_type != kChildEmulated && soc_type != kChildForked) {
    return ERROR_STACK(kErrorCodeProcRegisterUnsupportedSocType);
  }
  pre_registered_procs_.push_back(proc_and_name);
  // This is BEFORE the init, so we shouldn't use GLOG
  // LOG(INFO) << "pre-registered a user procedure: " << proc_and_name.first;
  return kRetOk;
}
ErrorStack  ProcManagerPimpl::local_register(const ProcAndName& proc_and_name) {
  if (!is_initialized()) {
    LOG(ERROR) << "Incorrect use of local_register(): "
      << get_error_message(kErrorCodeProcRegisterTooEarly);
    return ERROR_STACK(kErrorCodeProcRegisterTooEarly);
  }
  if (engine_->is_master()) {
    LOG(ERROR) << "Incorrect use of local_register(): "
      << get_error_message(kErrorCodeProcRegisterChildOnly);
    return ERROR_STACK(kErrorCodeProcRegisterChildOnly);
  }
  LocalProcId result = insert(proc_and_name, get_local_data());
  if (result == kLocalProcInvalid) {
    LOG(ERROR) << "A procedure of this name is already registered in this engine: "
      << proc_and_name.first;
    return ERROR_STACK(kErrorCodeProcProcAlreadyExists);
  }
  LOG(INFO) << "local-registered a user procedure: " << proc_and_name.first;
  return kRetOk;
}
ErrorStack  ProcManagerPimpl::emulated_register(const ProcAndName& proc_and_name) {
  if (!is_initialized()) {
    LOG(ERROR) << "Incorrect use of emulated_register(): "
      << get_error_message(kErrorCodeProcRegisterTooEarly);
    return ERROR_STACK(kErrorCodeProcRegisterTooEarly);
  }

  EngineType soc_type = engine_->get_options().soc_.soc_type_;
  if (soc_type != kChildEmulated) {
    LOG(ERROR) << "Incorrect use of emulated_register(): "
      << get_error_message(kErrorCodeProcRegisterUnsupportedSocType);
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
  return kLocalProcInvalid;
}

LocalProcId ProcManagerPimpl::insert(const ProcAndName& proc_and_name, SharedData* shared_data) {
  soc::SharedMutexScope lock_scope(&shared_data->control_block_->lock_);
  LocalProcId found = find_by_name(proc_and_name.first, shared_data);
  // TODO(Hideaki) max_proc_count check.
  if (found != kLocalProcInvalid) {
    return kLocalProcInvalid;
  }
  LocalProcId new_id = shared_data->control_block_->count_;
  shared_data->procs_[new_id] = proc_and_name;
  ++shared_data->control_block_->count_;
  // TODO(Hideaki) insert-sort to name_sort
  return new_id;
}

std::string ProcManagerPimpl::describe_registered_procs() const {
  if (engine_->is_master()) {
    return "<Master engine has no proc>";
  }
  std::stringstream str;
  const SharedData* data = get_local_data();
  str << "Proc Count=" << data->control_block_->count_ << ", (name,address)=[";
  for (LocalProcId i = 0; i < data->control_block_->count_; ++i) {
    if (i > 0) {
      str << ",";
    }
    str << "(" << data->procs_[i].first << "," << data->procs_[i].second << ")";
  }
  str << "]";

  return str.str();
}


}  // namespace proc
}  // namespace foedus
