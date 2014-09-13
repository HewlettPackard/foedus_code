/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/soc_manager_pimpl.hpp"

#include <spawn.h>
#include <stdlib.h>
#include <unistd.h>
#include <glog/logging.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/engine_type.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/proc/proc_manager.hpp"
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

  // shared memory allocated. now launch child SOCs
  child_emulated_engines_.clear();
  child_emulated_threads_.clear();
  std::memset(child_upids_, 0, sizeof(child_upids_));
  memory_repo_.change_master_status(MasterEngineStatus::kSharedMemoryAllocated);
  EngineType soc_type = engine_->get_options().soc_.soc_type_;
  if (soc_type == kChildForked) {
    CHECK_ERROR(launch_forked_children());
  } else if (soc_type == kChildLocalSpawned || soc_type == kChildRemoteSpawned) {
    // so far remote spawn does local spawn
    CHECK_ERROR(launch_spawned_children());
  } else {
    CHECK_ERROR(launch_emulated_children());
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

  CHECK_ERROR(wait_for_master_status(MasterEngineStatus::kWaitingForChildInitialization));
  return kRetOk;
}

ErrorStack SocManagerPimpl::wait_for_child_attach() {
  // As of this method, glog is not initialized. So, use std::cerr only.
  // When this method begins, the shared memory is in the most fragile state, not marked for
  // reclamation but needs to be open for shmget. Be careful in this method!
  // We avoid even assertions in this method.
  // As soon as something unusual happens, we conservatively release the shared memory.
  uint16_t soc_count = engine_->get_options().thread_.group_count_;

  // robustly wait until all children attached the shared memory.
  // mark-for-reclaim as soon as possible.
  const uint32_t kIntervalMillisecond = 20;
  const uint32_t kTimeoutMillisecond = 10000;
  uint32_t trials = 0;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kIntervalMillisecond));
    bool error_happened = false;
    bool remaining = false;
    for (uint16_t node = 0; node < soc_count; ++node) {
      ChildEngineStatus::StatusCode child_status = memory_repo_.get_child_status(node);
      switch (child_status) {
      case ChildEngineStatus::kInitial:
        remaining = true;
        break;
      case ChildEngineStatus::kSharedMemoryAttached:
        break;
      default:
        // all other statuses are unexpected
        error_happened = true;
        break;
      }
      // if we launched the child as a process, let's also check with waitpid()
      // this doesn't do anything if they are emulated children
      if (!error_happened && child_upids_[node] != 0) {
        int status = 0;
        pid_t wait_ret = ::waitpid(child_upids_[node], &status, WNOHANG);
        if (wait_ret == -1) {
          std::cerr << "[FOEDUS] FATAL! waitpid() for child-process " << child_upids_[node]
            << " failed." << std::endl;
          error_happened = true;
          break;
        } else if (WIFEXITED(status)) {
          std::cerr << "[FOEDUS] FATAL! child-process " << child_upids_[node] << " has exit"
            << " unexpectedly. status=" << status << std::endl;
          error_happened = true;
          break;
        } else if (WIFSIGNALED(status)) {
          std::cerr << "[FOEDUS] FATAL! child-process " << child_upids_[node] << " has been"
            << " terminated by signal. status=" << status << std::endl;
          error_happened = true;
          break;
        }
      }
    }

    if (error_happened) {
      std::cerr << "[FOEDUS] FATAL! Some child failed to attach shared memory." << std::endl;
      memory_repo_.change_master_status(MasterEngineStatus::kFatalError);
      memory_repo_.deallocate_shared_memories();
      return ERROR_STACK(kErrorCodeSocShmAttachFailed);
    } else if (!remaining) {
      break;  // done!
    } else if ((++trials) * kIntervalMillisecond > kTimeoutMillisecond) {
      std::cerr << "[FOEDUS] FATAL! Timeout happend while waiting for child SOCs to start up."
        " Probably child SOC(s) hanged or did not trap SOC execution (if spawned)." << std::endl;
      memory_repo_.change_master_status(MasterEngineStatus::kFatalError);
      memory_repo_.deallocate_shared_memories();
      return ERROR_STACK(kErrorCodeSocLaunchTimeout);
    }
  }

  // as soon as all children ack-ed, mark the shared memory for release.
  // no one will newly issue shmget.
  memory_repo_.change_master_status(MasterEngineStatus::kSharedMemoryReservedReclamation);
  memory_repo_.mark_for_release();  // now it's safe. closed attaching and marked for reclaim.
  return kRetOk;
}

ErrorStack SocManagerPimpl::wait_for_master_status(MasterEngineStatus::StatusCode target_status) {
  ASSERT_ND(!engine_->is_master());
  const uint32_t kIntervalMillisecond = 10;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kIntervalMillisecond));
    MasterEngineStatus::StatusCode master_status = memory_repo_.get_master_status();
    if (master_status == target_status) {
      break;  // wait done
    } else if (master_status == MasterEngineStatus::kFatalError) {
      return ERROR_STACK(kErrorCodeSocMasterDied);
    } else if (static_cast<int>(master_status) > static_cast<int>(target_status)) {
      return ERROR_STACK(kErrorCodeSocMasterUnexpectedState);
    }

    // Also check parent's process status.
    Upid master_upid = engine_->get_master_upid();
    int status = 0;
    pid_t wait_ret = ::waitpid(master_upid, &status, WNOHANG);
    if (wait_ret == -1) {
      std::cerr << "[FOEDUS-Child] FATAL! waitpid() for master-process " << master_upid
        << " failed." << std::endl;
      return ERROR_STACK(kErrorCodeSocMasterDied);
    } else if (WIFEXITED(status)) {
      std::cerr << "[FOEDUS-Child] FATAL! master-process " << master_upid << " has exit"
        << " unexpectedly. status=" << status << std::endl;
      return ERROR_STACK(kErrorCodeSocMasterDied);
    } else if (WIFSIGNALED(status)) {
      std::cerr << "[FOEDUS-Child] FATAL! master-process " << master_upid << " has been"
        << " terminated by signal. status=" << status << std::endl;
      return ERROR_STACK(kErrorCodeSocMasterDied);
    }
  }
  return kRetOk;
}

////////////////////////////////////////////////////////////////////////////////////
//
//              Child SOCs: Emulate
//
////////////////////////////////////////////////////////////////////////////////////
ErrorStack SocManagerPimpl::launch_emulated_children() {
  uint16_t soc_count = engine_->get_options().thread_.group_count_;
  for (uint16_t node = 0; node < soc_count; ++node) {
    child_emulated_engines_.push_back(nullptr);
  }
  // from now on child_emulated_engines_ doesn't grow/shrink
  for (uint16_t node = 0; node < soc_count; ++node) {
    child_emulated_threads_.emplace_back(std::thread(
      &SocManagerPimpl::emulated_child_main,
      this,
      node));
  }

  CHECK_ERROR(wait_for_child_attach());
  return kRetOk;
}

void SocManagerPimpl::emulated_child_main(SocId node) {
  Upid master_upid = engine_->get_master_upid();
  Engine soc_engine(kChildEmulated, master_upid, node);
  child_emulated_engines_[node] = &soc_engine;
  ErrorStack init_error = soc_engine.initialize();
  if (init_error.is_error()) {
    std::cerr << "[FOEDUS-Child] Failed to initialize emulated child SOC. error=" << init_error
      << " This is an unrecoverable error. This thread quits shortly" << std::endl;
    soc_engine.uninitialize();
    child_emulated_engines_[node] = nullptr;
    return;
  }

  SocManagerPimpl* soc_this = soc_engine.get_soc_manager().pimpl_;
  SharedMemoryRepo& soc_memory = soc_this->memory_repo_;

  // after initialize(), we can safely use glog.
  LOG(INFO) << "The emulated SOC engine-" << node << " was initialized.";

  // We can reuse procedures pre-registered in master. Good for being a thread.
  const auto& procedures = engine_->get_proc_manager().get_pre_registered_procedures();
  for (const auto& procedure : procedures) {
    soc_engine.get_proc_manager().local_register(procedure);
  }

  LOG(INFO) << "Waiting for master engine's initialization...";
  soc_memory.change_child_status(node, ChildEngineStatus::kWaitingForMasterInitialization);
  COERCE_ERROR(soc_this->wait_for_master_status(MasterEngineStatus::kRunning));
  LOG(INFO) << "The emulated SOC engine-" << node << " detected that master engine has started"
    << " running.";
  soc_memory.change_child_status(node, ChildEngineStatus::kRunning);
  COERCE_ERROR(soc_this->wait_for_master_status(MasterEngineStatus::kWaitingForChildTerminate));

  LOG(INFO) << "Stopping the emulated SOC engine-" << node;
  ErrorStack uninit_error = soc_engine.uninitialize();
  child_emulated_engines_[node] = nullptr;
  if (uninit_error.is_error()) {
    LOG(ERROR) << "Error while uninitializing emulated SOC engine-" << node << ": " << uninit_error;
  }
}

////////////////////////////////////////////////////////////////////////////////////
//
//              Child SOCs: fork
//
////////////////////////////////////////////////////////////////////////////////////
ErrorStack SocManagerPimpl::launch_forked_children() {
  uint16_t soc_count = engine_->get_options().thread_.group_count_;
  for (uint16_t node = 0; node < soc_count; ++node) {
    pid_t pid = ::fork();
    if (pid == -1) {
      // failed to fork! immediately release shared memory and return.
      memory_repo_.change_master_status(MasterEngineStatus::kFatalError);
      memory_repo_.deallocate_shared_memories();
      std::cerr << "[FOEDUS] Failed to fork child SOC. error=" << assorted::os_error() << std::endl;
      // the already forked children will shortly notice that the parent failed and exits.
      return ERROR_STACK(kErrorCodeSocForkFailed);
    } else if (pid == 0) {
      // child
      int child_ret = forked_child_main(node);
      ::_exit(child_ret);
    } else {
      // parent. move on.
      child_upids_[node] = pid;
    }
  }
  CHECK_ERROR(wait_for_child_attach());
  return kRetOk;
}

int SocManagerPimpl::forked_child_main(SocId node) {
  Upid master_upid = engine_->get_master_upid();
  Engine soc_engine(kChildForked, master_upid, node);
  ErrorStack init_error = soc_engine.initialize();
  if (init_error.is_error()) {
    std::cerr << "[FOEDUS-Child] Failed to initialize forked child SOC. error=" << init_error
      << " This is an unrecoverable error. This process quits shortly" << std::endl;
    soc_engine.uninitialize();
    return EXIT_FAILURE;
  }

  SocManagerPimpl* soc_this = soc_engine.get_soc_manager().pimpl_;
  SharedMemoryRepo& soc_memory = soc_this->memory_repo_;

  // after initialize(), we can safely use glog.
  LOG(INFO) << "The forked SOC engine-" << node << " was initialized.";

  // These are pre-registered before fork(), so still we can reuse.
  const auto& procedures = engine_->get_proc_manager().get_pre_registered_procedures();
  for (const auto& procedure : procedures) {
    soc_engine.get_proc_manager().local_register(procedure);
  }

  LOG(INFO) << "Waiting for master engine's initialization...";
  soc_memory.change_child_status(node, ChildEngineStatus::kWaitingForMasterInitialization);
  COERCE_ERROR(soc_this->wait_for_master_status(MasterEngineStatus::kRunning));
  LOG(INFO) << "The forked SOC engine-" << node << " detected that master engine has started"
    << " running.";
  soc_memory.change_child_status(node, ChildEngineStatus::kRunning);
  COERCE_ERROR(soc_this->wait_for_master_status(MasterEngineStatus::kWaitingForChildTerminate));

  LOG(INFO) << "Stopping the forked SOC engine-" << node;
  ErrorStack uninit_error = soc_engine.uninitialize();
  if (uninit_error.is_error()) {
    LOG(ERROR) << "Error while uninitializing forked SOC engine-" << node << ": " << uninit_error;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////
//
//              Child SOCs: spawn
//
////////////////////////////////////////////////////////////////////////////////////
ErrorStack SocManagerPimpl::launch_spawned_children() {
  Upid master_upid = engine_->get_master_upid();
  uint16_t soc_count = engine_->get_options().thread_.group_count_;
  for (uint16_t node = 0; node < soc_count; ++node) {
    posix_spawn_file_actions_t file_actions;
    posix_spawnattr_t attr;
    ::posix_spawn_file_actions_init(&file_actions);
    ::posix_spawnattr_init(&attr);
    std::string executable = engine_->get_options().soc_.convert_spawn_executable_pattern(node);
    std::string ld_path = engine_->get_options().soc_.convert_spawn_ld_library_path_pattern(node);
    std::string ld_env("LD_LIBRARY_PATH=");
    ld_env += ld_path;
    std::string pid_env("FOEDUS_MASTER_UPID=");
    pid_env += std::to_string(master_upid);
    std::string soc_id_env("FOEDUS_SOC_ID=");
    soc_id_env += std::to_string(node);

    char* const argv[] = { const_cast<char*>(executable.c_str()), nullptr};
    char* const envp[] = {
      const_cast<char*>(ld_env.c_str()),
      const_cast<char*>(pid_env.c_str()),
      const_cast<char*>(soc_id_env.c_str()),
      nullptr};

    pid_t child_pid;
    int ret = ::posix_spawn(&child_pid, executable.c_str(), &file_actions, &attr, argv, envp);
    if (ret == -1) {
      // failed to spawn! immediately release shared memory and return.
      memory_repo_.change_master_status(MasterEngineStatus::kFatalError);
      memory_repo_.deallocate_shared_memories();
      std::cerr << "[FOEDUS] Failed to spawn child SOC. error="
        << assorted::os_error() << std::endl;
      // the already spawned children will shortly notice that the parent failed and exits.
      return ERROR_STACK(kErrorCodeSocSpawnFailed);
    }
    child_upids_[node] = child_pid;
  }
  CHECK_ERROR(wait_for_child_attach());
  return kRetOk;
}

void SocManagerPimpl::spawned_child_main(const std::vector< proc::ProcAndName >& procedures) {
  const char* master_upid_str = std::getenv("FOEDUS_MASTER_UPID");
  const char* soc_id_str = std::getenv("FOEDUS_SOC_ID");
  if (master_upid_str == nullptr || soc_id_str == nullptr) {
    return;  // not launched as an SOC engine. exit
  }

  Upid master_upid = std::atoll(master_upid_str);
  SocId node = std::atol(master_upid_str);
  Engine soc_engine(kChildLocalSpawned, master_upid, node);
  ErrorStack init_error = soc_engine.initialize();
  if (init_error.is_error()) {
    std::cerr << "[FOEDUS-Child] Failed to initialize spawned child SOC. error=" << init_error
      << " This is an unrecoverable error. This process quits shortly" << std::endl;
    soc_engine.uninitialize();
    ::_exit(EXIT_FAILURE);
  }

  SocManagerPimpl* soc_this = soc_engine.get_soc_manager().pimpl_;
  SharedMemoryRepo& soc_memory = soc_this->memory_repo_;

  // after initialize(), we can safely use glog.
  LOG(INFO) << "The spawned SOC engine-" << node << " was initialized.";

  // Add the given procedures. This is the benefit of spawn-type SOCs
  for (const proc::ProcAndName& proc_and_name : procedures) {
    soc_engine.get_proc_manager().local_register(proc_and_name);
  }

  LOG(INFO) << "Added user procedures. Waiting for master engine's initialization...";
  soc_memory.change_child_status(node, ChildEngineStatus::kWaitingForMasterInitialization);
  COERCE_ERROR(soc_this->wait_for_master_status(MasterEngineStatus::kRunning));
  LOG(INFO) << "The spawned SOC engine-" << node << " detected that master engine has started"
    << " running.";
  soc_memory.change_child_status(node, ChildEngineStatus::kRunning);
  COERCE_ERROR(soc_this->wait_for_master_status(MasterEngineStatus::kWaitingForChildTerminate));

  LOG(INFO) << "Stopping the spawned SOC engine-" << node;
  soc_memory.change_child_status(node, ChildEngineStatus::kTerminated);
  ErrorStack uninit_error = soc_engine.uninitialize();
  if (uninit_error.is_error()) {
    LOG(ERROR) << "Error while uninitializing spawned SOC engine-" << node << ": " << uninit_error;
    ::_exit(EXIT_FAILURE);
  }

  ::_exit(EXIT_SUCCESS);
}


}  // namespace soc
}  // namespace foedus
