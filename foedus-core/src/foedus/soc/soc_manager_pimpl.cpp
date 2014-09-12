/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/soc_manager_pimpl.hpp"

#include <glog/logging.h>

#include <iostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/engine_type.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/soc/soc_manager.hpp"

namespace foedus {
namespace soc {
// SOC manager is initialized at first even before debug module.
// So, we can't use glog yet.
ErrorStack SocManagerPimpl::initialize_once() {
  return kRetOk;
}

ErrorStack SocManagerPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  memory_repo_.deallocate_shared_memories();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack SocManagerPimpl::initialize_master() {
  ErrorStack alloc_error = memory_repo_.allocate_shared_memories(engine_->get_options());
  if (alloc_error.is_error()) {
    memory_repo_.deallocate_shared_memories();
    return alloc_error;
  }

  return kRetOk;
}

ErrorStack SocManagerPimpl::initialize_child() {
  Upid master_upid = engine_->get_master_upid();
  SocId soc_id = engine_->get_soc_id();
  pid_t pid = ::getpid();
  ErrorStack attach_error = memory_repo_.attach_shared_memories(
    master_upid,
    soc_id,
    engine_->get_nonconst_options());
  if (attach_error.is_error()) {
    std::cerr << "[FOEDUS-Child] MasterUpid=" << master_upid << ", ChildPid=" << pid
      << ", Node=" << soc_id << ". Failed to attach shared memory. error=" << attach_error
      << " This is an unrecoverable error. This process quits shortly" << std::endl;
    ::_exit(1);
  }

  return kRetOk;
}


void SocManagerPimpl::launch_emulated_children() {
  uint16_t soc_count = engine_->get_options().thread_.group_count_;
  for (uint16_t node = 0; node < soc_count; ++node) {
    child_emulated_threads_.emplace_back(std::thread(
      SocManagerPimpl::emulated_child_main,
      engine_,
      node));
  }
}

void SocManagerPimpl::emulated_child_main(Engine* master_engine, SocId node) {
  Upid master_upid = master_engine->get_master_upid();
  SharedMemoryRepo* master_memory = master_engine->get_soc_manager().get_shared_memory_repo();
  Engine soc_engine(kChildEmulated, master_upid, node);
  ErrorStack init_error = soc_engine.initialize();
  if (init_error.is_error()) {
    std::cerr << "[FOEDUS-Child] Failed to initialize emulated child SOC. error=" << init_error
      << " This is an unrecoverable error. This thread quits shortly" << std::endl;
    soc_engine.uninitialize();
    return;
  }

  // after initialize(), we can safely use glog.
  LOG(INFO) << "The emulated SOC engine-" << node << " was initialized. Started execution..";
  while (true) {
  }

  LOG(INFO) << "Stopping the emulated SOC engine-" << node;
  ErrorStack uninit_error = soc_engine.uninitialize();
  if (uninit_error.is_error()) {
    LOG(ERROR) << "Error while uninitializing emulated SOC engine-" << node << ": " << uninit_error;
  }
}


}  // namespace soc
}  // namespace foedus
