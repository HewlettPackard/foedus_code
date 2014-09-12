/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_HPP_
#define FOEDUS_ENGINE_HPP_

#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/engine_type.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/initializable.hpp"
#include "foedus/debugging/fwd.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/proc/fwd.hpp"
#include "foedus/restart/fwd.hpp"
#include "foedus/savepoint/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/soc/fwd.hpp"
#include "foedus/soc/soc_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
namespace foedus {

// forward declarations
class EnginePimpl;
class EngineOptions;

/**
 * @defgroup COMPONENTS FOEDUS Components
 * @brief Main modules of libfoedus.
 */

/**
 * @defgroup ENGINE Database Engine
 * @ingroup COMPONENTS
 * @brief \b Database \b Engine, the top-level component of foedus.
 * @details
 * bluh
 * @section MODDEP Module Dependency
 * Sub-modules in the engine have dependencies between them.
 * For example, all other sub-modules depend on \ref DEBUGGING, so the debugging module
 * is initialized at first, and uninitialized at end.
 * There must not be a cycle, obviously. Below is the list of dependencies
 *  \li \ref SOC must start up at the beginning as this is a special module.
 *  \li All other modules depend on \ref DEBUGGING.
 *  \li \ref PROC depends on \ref DEBUGGING and \ref SOC.
 *  \li \ref THREAD depend on \ref MEMORY.
 *  \li \ref LOG depend on \ref THREAD and \ref SAVEPOINT.
 *  \li \ref SNAPSHOT and \ref CACHE depend on \ref LOG.
 *  \li \ref STORAGE depend on \ref SNAPSHOT and \ref CACHE.
 *  \li \ref XCT depends on \ref STORAGE.
 *  \li \ref RESTART depends on everything else (and runs only at restart by definition).
 *
 * (transitively implied dependencies omitted, eg \ref LOG of course depends on \ref MEMORY).
 *
 * @msc
 * SOC,DBG,PROC,MEM,SP,THREAD,LOG,SNAPSHOT,CACHE,STORAGE,XCT,RESTART;
 * SOC<=DBG;
 * DBG<=PROC;
 * SOC<=PROC;
 * DBG<=MEM;
 * DBG<=SP;
 * MEM<=THREAD;
 * THREAD<=LOG;
 * SP<=LOG;
 * LOG<=SNAPSHOT;
 * LOG<=CACHE;
 * SNAPSHOT<=STORAGE;
 * CACHE<=STORAGE;
 * STORAGE<=XCT;
 * MEM<=RESTART;
 * THREAD<=RESTART;
 * LOG<=RESTART;
 * SNAPSHOT<=RESTART;
 * CACHE<=RESTART;
 * STORAGE<=RESTART;
 * XCT<=RESTART;
 * @endmsc
 *
 * Hence, we initialize/uninitialize the modules in the above order.
 */

/**
 * @brief Database engine object that holds all resources and provides APIs.
 * @ingroup ENGINE
 * @details
 * Detailed description of this class.
 */
class Engine CXX11_FINAL : public virtual Initializable {
 public:
  /**
   * @brief Instantiates a \b master engine object which is \b NOT initialized yet.
   * @param[in] options Configuration of the engine
   * @details
   * To start the engine, call initialize() afterwards.
   * This constructor dose nothing but instantiation.
   */
  explicit Engine(const EngineOptions &options);

  /**
   * @brief Instantiates a \b child engine object which is \b NOT initialized yet.
   * @param[in] type Launch type of the child engine object
   * @param[in] master_upid Universal (or Unique) ID of the master process.
   * @param[in] soc_id SOC-ID (or NUMA-node) of the child engine.
   * @details
   * Use this constructor only when you modify your main() function to capture kChildLocalSpawned
   * launch. Otherwise, this constructor is used only internally.
   */
  Engine(EngineType type, soc::Upid master_upid, soc::SocId soc_id);

  /**
   * @brief Do NOT rely on this destructor to release resources. Call uninitialize() instead.
   * @details
   * If this destructor is called before the call of uninitialize(), there was something wrong.
   * So, this destructor complains about it in stderr if that's the case.
   * Remember, destructor is not the best place to do complex things. Always use uninitialize()
   * for better handling of unexpected errors.
   */
  ~Engine();

  // Disable default constructors
  Engine() CXX11_FUNC_DELETE;
  Engine(const Engine &) CXX11_FUNC_DELETE;
  Engine& operator=(const Engine &) CXX11_FUNC_DELETE;

  std::string describe_short() const;

  /**
   * Starts up the database engine. This is the first method to call.
   * @see Initializable#initialize()
   */
  ErrorStack  initialize() CXX11_OVERRIDE;

  /**
   * Returns whether the engine is currently running.
   * @see Initializable#is_initialized()
   */
  bool        is_initialized() const CXX11_OVERRIDE;

  /**
   * Terminates the database engine. This is the last method to call.
   * @see Initializable#uninitialize()
   */
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /** @see EngineOptions */
  const EngineOptions&            get_options() const;
  /** See \ref DEBUGGING */
  debugging::DebuggingSupports&   get_debug() const;
  /** See \ref LOG */
  log::LogManager&                get_log_manager() const;
  /** See \ref MEMORY */
  memory::EngineMemory&           get_memory_manager() const;
  /** See \ref PROC */
  proc::ProcManager&              get_proc_manager() const;
  /** See \ref THREAD */
  thread::ThreadPool&             get_thread_pool() const;
  /** See \ref SAVEPOINT */
  savepoint::SavepointManager&    get_savepoint_manager() const;
  /** See \ref SNAPSHOT */
  snapshot::SnapshotManager&      get_snapshot_manager() const;
  /** See \ref SOC */
  soc::SocManager&                get_soc_manager() const;
  /** See \ref STORAGE */
  storage::StorageManager&        get_storage_manager() const;
  /** See \ref XCT */
  xct::XctManager&                get_xct_manager() const;
  /** See \ref RESTART */
  restart::RestartManager&        get_restart_manager() const;

  /** Returns the type of this engine object. */
  EngineType  get_type() const;
  /** Returns if this engine object is a master instance */
  bool        is_master() const;
  /** Returns if this engine object is a child instance running just as a thread */
  bool        is_emulated_child() const;
  /** Returns if this engine object is a child instance launched by fork */
  bool        is_forked_child() const;
  /** Returns if this engine object is a child instance launched by spawn */
  bool        is_local_spawned_child() const;
  /** Returns if this engine object is a child instance remotely launched by spawn */
  bool        is_remote_spawned_child() const;
  /** If this is a child instance, returns its SOC ID (NUMA node). Otherwise always 0. */
  soc::SocId  get_soc_id() const;
  /** Returns Universal (or Unique) ID of the master process. */
  soc::Upid   get_master_upid() const;

  /**
   * Returns an updatable reference to options.
   * This must be used only by SocManager during initialization.
   * Users must not call this method.
   */
  EngineOptions*                  get_nonconst_options();

 private:
  EnginePimpl* pimpl_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_HPP_
