/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_PIMPL_HPP_
#define FOEDUS_ENGINE_PIMPL_HPP_

#include <string>
#include <vector>

#include "foedus/engine_options.hpp"
#include "foedus/engine_type.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
// This is pimpl. no need for further indirections. just include them all.
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/restart/restart_manager.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/soc/soc_id.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
/**
 * @brief Pimpl object of Engine.
 * @ingroup ENGINE
 * @details
 * A private pimpl object for Engine.
 * Do not include this header from a client program unless you know what you are doing.
 */
class EnginePimpl final : public DefaultInitializable {
 public:
  EnginePimpl() = delete;
  EnginePimpl(Engine* engine, const EngineOptions &options);
  EnginePimpl(Engine* engine, EngineType type, soc::Upid master_upid, soc::SocId soc_id);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;
  ErrorStack  check_valid_options();

  /** Options given at boot time. Immutable once launched */
  EngineOptions                   options_;

  /** Pointer to the enclosing object. Few places would need it, but hold it in case. */
  Engine* const                   engine_;

  const EngineType                type_;
  const soc::Upid                 master_upid_;
  const soc::SocId                soc_id_;

// Individual modules. Placed in initialize()/uninitialize() order
// (remember, modules have dependencies between them).

  /**
   * SOC manager.
   * This is a quite special module that launches child SOC engines.
   * We have to initialize this module before everything else, even before debug_
   * (glog 0.3.3 has an issue across fork(), see
   *   https://code.google.com/p/google-glog/issues/detail?id=101
   *   https://code.google.com/p/google-glog/issues/detail?id=82
   * ).
   */
  soc::SocManager                 soc_manager_;
  /**
   * Debugging supports.
   * @attention Because this module initializes/uninitializes basic debug logging support,
   * EnginePimpl#initialize_once() must initialize it at the beginning,
   * and EnginePimpl#uninitialize_once() must uninitialize it at the end.
   * We should not use glog (except when we have to, in which case glog will give warnings)
   * before and after that.
   */
  debugging::DebuggingSupports    debug_;
  proc::ProcManager               proc_manager_;
  memory::EngineMemory            memory_manager_;
  savepoint::SavepointManager     savepoint_manager_;
  thread::ThreadPool              thread_pool_;
  log::LogManager                 log_manager_;
  snapshot::SnapshotManager       snapshot_manager_;
  storage::StorageManager         storage_manager_;
  xct::XctManager                 xct_manager_;
  restart::RestartManager         restart_manager_;

  /** Returns in \e initialization order. */
  std::vector< Initializable* > get_children() {
    std::vector< Initializable* > children;
    children.push_back(&soc_manager_);
    children.push_back(&debug_);
    children.push_back(&proc_manager_);
    children.push_back(&memory_manager_);
    children.push_back(&savepoint_manager_);
    children.push_back(&thread_pool_);
    children.push_back(&log_manager_);
    children.push_back(&snapshot_manager_);
    children.push_back(&storage_manager_);
    children.push_back(&xct_manager_);
    children.push_back(&restart_manager_);
    return children;
  }
  std::string describe_short() const;

 private:
  /**
   * THP being disabled is one of the most frequent misconfiguration that reduces performance
   * for 30% or more. We output a strong warning at startup if it's not "always" mode.
   */
  static ErrorStack check_transparent_hugepage_setting();
  ErrorStack check_minimal_pool_size() const;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_PIMPL_HPP_
