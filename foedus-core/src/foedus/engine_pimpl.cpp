/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/engine_pimpl.hpp"

#include <unistd.h>
#include <valgrind.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include "foedus/error_stack_batch.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

namespace foedus {

EnginePimpl::EnginePimpl(Engine* engine, const EngineOptions &options) :
  options_(options),
  engine_(engine),
  type_(kMaster),
  master_upid_(::getpid()),
  soc_id_(0),
  // although we give a pointer to engine, these objects must not access it yet.
  // even the Engine object has not set the pimpl pointer.
  soc_manager_(engine),
  debug_(engine),
  proc_manager_(engine),
  memory_manager_(engine),
  savepoint_manager_(engine),
  thread_pool_(engine),
  log_manager_(engine),
  snapshot_manager_(engine),
  storage_manager_(engine),
  xct_manager_(engine),
  restart_manager_(engine) {
}
EnginePimpl::EnginePimpl(
  Engine* engine,
  EngineType type,
  soc::Upid master_upid,
  soc::SocId soc_id) :
  engine_(engine),
  type_(type),
  master_upid_(master_upid),
  soc_id_(soc_id),
  soc_manager_(engine),
  debug_(engine),
  proc_manager_(engine),
  memory_manager_(engine),
  savepoint_manager_(engine),
  thread_pool_(engine),
  log_manager_(engine),
  snapshot_manager_(engine),
  storage_manager_(engine),
  xct_manager_(engine),
  restart_manager_(engine) {
}

std::string EnginePimpl::describe_short() const {
  if (type_ == kMaster) {
    return "MASTER";
  }
  std::string ret("CHILD-");
  return ret + std::to_string(soc_id_);
}

ErrorStack EnginePimpl::initialize_once() {
  if (is_master()) {
    CHECK_ERROR(check_valid_options());
  }
  // SOC manager is special. We must initialize it first.
  CHECK_ERROR(soc_manager_.initialize());
  on_module_initialized(kSoc);

  // The following can assume SOC manager is already initialized
  for (ModulePtr& module : get_modules()) {
    // During initialization, SOCs wait for master's initialization before their init.
    if (!is_master()) {
      CHECK_ERROR(soc_manager_.wait_for_master_module(true, module.type_));
    }
    CHECK_ERROR(module.ptr_->initialize());
    on_module_initialized(module.type_);
    // Then master waits for SOCs before moving on to next module.
    if (is_master()) {
      CHECK_ERROR(soc_manager_.wait_for_children_module(true, module.type_));
    }
  }
  if (is_master()) {
    soc::SharedMemoryRepo* repo = soc_manager_.get_shared_memory_repo();
    repo->change_master_status(soc::MasterEngineStatus::kRunning);
    // wait for children's kRunning status
    // TODO(Hideaki) should be a function in soc manager
    uint16_t soc_count = engine_->get_options().thread_.group_count_;
    while (true) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      assorted::memory_fence_acq_rel();
      bool error_happened = false;
      bool remaining = false;
      for (uint16_t node = 0; node < soc_count; ++node) {
        soc::ChildEngineStatus* status = repo->get_node_memory_anchors(node)->child_status_memory_;
        if (status->status_code_ == soc::ChildEngineStatus::kFatalError) {
          error_happened = true;
          break;
        }
        if (status->status_code_ == soc::ChildEngineStatus::kRunning) {
          continue;  // ok
        }
        remaining = true;
      }

      if (error_happened) {
        LOG(ERROR) << "[FOEDUS] ERROR! error while waiting child kRunning";
        return ERROR_STACK(kErrorCodeSocChildInitFailed);
      } else if (!remaining) {
        break;
      }
    }
  }
  LOG(INFO) << "================================================================================";
  LOG(INFO) << "================== FOEDUS ENGINE ("
    << describe_short() << ") INITIALIZATION DONE ===========";
  LOG(INFO) << "================================================================================";

  // In a few places, we check if we are running under valgrind and, if so, turn off
  // optimizations valgrind can't handle (eg hugepages).
  bool running_on_valgrind = RUNNING_ON_VALGRIND;
  if (running_on_valgrind) {
    LOG(INFO) << "=============== ATTENTION: VALGRIND MODE! ==================";
    LOG(INFO) << "This Engine is running under valgrind, which disables several optimizations";
    LOG(INFO) << "If you see this message while usual execution, something is wrong.";
    LOG(INFO) << "=============== ATTENTION: VALGRIND MODE! ==================";
  }
  return kRetOk;
}
ErrorStack EnginePimpl::uninitialize_once() {
  LOG(INFO) << "================================================================================";
  LOG(INFO) << "=================== FOEDUS ENGINE ("
    << describe_short() << ") EXITTING...... ================";
  LOG(INFO) << "================================================================================";
  if (is_master()) {
    soc_manager_.get_shared_memory_repo()->change_master_status(
      soc::MasterEngineStatus::kWaitingForChildTerminate);
  }
  ErrorStackBatch batch;
  // uninit in reverse order of initialization
  auto modules = get_modules();
  std::reverse(modules.begin(), modules.end());
  for (ModulePtr& module : modules) {
    // During uninitialization, master waits for SOCs' uninitialization before its uninit.
    if (is_master()) {
      batch.emprace_back(soc_manager_.wait_for_children_module(false, module.type_));
    }
    batch.emprace_back(module.ptr_->uninitialize());
    on_module_uninitialized(module.type_);
    // Then SOCs wait for master before moving on to next module.
    if (!is_master()) {
      batch.emprace_back(soc_manager_.wait_for_master_module(false, module.type_));
    }
  }

  // SOC manager is special. We must uninitialize it at last.
  batch.emprace_back(soc_manager_.uninitialize());
  // after that, we can't even set status. shared memory has been detached.
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack EnginePimpl::check_valid_options() {
  CHECK_ERROR(check_minimal_pool_size());
  CHECK_ERROR(check_transparent_hugepage_setting());
  return kRetOk;
}

ErrorStack EnginePimpl::check_minimal_pool_size() const {
  // Can we at least start up?
  const thread::ThreadOptions& t = options_.thread_;
  const memory::MemoryOptions& m = options_.memory_;
  uint64_t total_threads = t.group_count_ * t.thread_count_per_group_;
  uint64_t minimal_page_pool
    = total_threads * m.private_page_pool_initial_grab_ * storage::kPageSize;
  if ((static_cast<uint64_t>(m.page_pool_size_mb_per_node_)
      * t.group_count_ << 20) < minimal_page_pool) {
    return ERROR_STACK(kErrorCodeMemoryPagePoolTooSmall);
  }
  return kRetOk;
}

ErrorStack EnginePimpl::check_transparent_hugepage_setting() {
  /* we don't warn about THP anymore. We anyway use pre-allocated hugepages
  std::ifstream conf("/sys/kernel/mm/transparent_hugepage/enabled");
  if (conf.is_open()) {
    std::string line;
    std::getline(conf, line);
    conf.close();
    if (line == "[always] madvise never") {
      std::cout << "Great, THP is in always mode" << std::endl;
    } else {
      // Now that we use non-transparent hugepages rather than THP, we don't output this as
      // warning. Maybe we completely get rid of this message.
      std::cerr << "THP is not in always mode ('" << line << "')."
        << " Not enabling THP reduces our performance up to 30%. Run the following to enable it:"
        << std::endl << "  sudo sh -c 'echo always > /sys/kernel/mm/transparent_hugepage/enabled'"
        << std::endl;
    }
    return kRetOk;
  }

  std::cerr << "Could not read /sys/kernel/mm/transparent_hugepage/enabled to check"
    << " if THP is enabled. This implies that THP is not available in this system."
    << " Using an old linux without THP reduces our performance up to 30%" << std::endl;
  */
  return kRetOk;
}

void EnginePimpl::on_module_initialized(ModuleType module) {
  soc::SharedMemoryRepo* repo = soc_manager_.get_shared_memory_repo();
  if (is_master()) {
    repo->get_global_memory_anchors()->master_status_memory_->change_init_atomic(module);
  } else {
    repo->get_node_memory_anchors(soc_id_)->child_status_memory_->change_init_atomic(module);
  }
}

void EnginePimpl::on_module_uninitialized(ModuleType module) {
  soc::SharedMemoryRepo* repo = soc_manager_.get_shared_memory_repo();
  if (is_master()) {
    repo->get_global_memory_anchors()->master_status_memory_->change_uninit_atomic(module);
  } else {
    repo->get_node_memory_anchors(soc_id_)->child_status_memory_->change_uninit_atomic(module);
  }
}

}  // namespace foedus
