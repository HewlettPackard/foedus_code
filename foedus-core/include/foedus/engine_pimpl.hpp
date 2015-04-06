/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_ENGINE_PIMPL_HPP_
#define FOEDUS_ENGINE_PIMPL_HPP_

#include <string>
#include <vector>

#include "foedus/engine_options.hpp"
#include "foedus/engine_type.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/module_type.hpp"
// This is pimpl. no need for further indirections. just include them all.
#include "foedus/cache/cache_manager.hpp"
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
  /** pair of module pointer and its type. */
  struct ModulePtr {
    ModulePtr() : ptr_(nullptr), type_(kInvalid) {}
    ModulePtr(Initializable* ptr, ModuleType type) : ptr_(ptr), type_(type) {}
    Initializable* ptr_;
    ModuleType type_;
  };

  EnginePimpl() = delete;
  EnginePimpl(Engine* engine, const EngineOptions &options);
  EnginePimpl(
    Engine* engine,
    EngineType type,
    soc::Upid master_upid,
    Eid master_eid,
    soc::SocId soc_id);

  bool        is_master() const { return type_ == kMaster; }
  ErrorStack  initialize_once() override;
  ErrorStack  initialize_modules();
  ErrorStack  uninitialize_once() override;
  ErrorStack  check_valid_options();

  /** Called whenever each module has completed its initialization. */
  void        on_module_initialized(ModuleType module);
  /** Called whenever each module has completed its uninitialization. */
  void        on_module_uninitialized(ModuleType module);

  /** Options given at boot time. Immutable once launched */
  EngineOptions                   options_;

  /** Pointer to the enclosing object. Few places would need it, but hold it in case. */
  Engine* const                   engine_;

  const EngineType                type_;
  const soc::Upid                 master_upid_;
  const Eid                       master_eid_;
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
  cache::CacheManager             cache_manager_;
  storage::StorageManager         storage_manager_;
  xct::XctManager                 xct_manager_;
  restart::RestartManager         restart_manager_;

  /** Returns in \e initialization order. */
  std::vector< ModulePtr > get_modules() {
    std::vector< ModulePtr > modules;
    // modules.push_back(ModulePtr(&soc_manager_, kSoc));  SOC Manager is specially inited/uninited
    modules.push_back(ModulePtr(&debug_, kDebug));
    modules.push_back(ModulePtr(&proc_manager_, kProc));
    modules.push_back(ModulePtr(&memory_manager_, kMemory));
    modules.push_back(ModulePtr(&savepoint_manager_, kSavepoint));
    modules.push_back(ModulePtr(&thread_pool_, kThread));
    modules.push_back(ModulePtr(&log_manager_, kLog));
    modules.push_back(ModulePtr(&snapshot_manager_, kSnapshot));
    modules.push_back(ModulePtr(&cache_manager_, kCache));
    modules.push_back(ModulePtr(&storage_manager_, kStorage));
    modules.push_back(ModulePtr(&xct_manager_, kXct));
    modules.push_back(ModulePtr(&restart_manager_, kRestart));
    return modules;
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
