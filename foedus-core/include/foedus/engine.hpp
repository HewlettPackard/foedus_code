/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_HPP_
#define FOEDUS_ENGINE_HPP_

#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/initializable.hpp>
#include <foedus/debugging/fwd.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/restart/fwd.hpp>
#include <foedus/savepoint/fwd.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/storage/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/xct/fwd.hpp>
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
 *  \li All modules depend on \ref DEBUGGING.
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
 * DBG,MEM,SP,THREAD,LOG,SNAPSHOT,CACHE,STORAGE,XCT,RESTART;
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
   * @brief Instantiates an engine object which is \b NOT initialized yet.
   * @details
   * To start the engine, call initialize() afterwards.
   * This constructor dose nothing but instantiation.
   */
  explicit Engine(const EngineOptions &options);

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
  /** See \ref THREAD */
  thread::ThreadPool&             get_thread_pool() const;
  /** See \ref SAVEPOINT */
  savepoint::SavepointManager&    get_savepoint_manager() const;
  /** See \ref SNAPSHOT */
  snapshot::SnapshotManager&      get_snapshot_manager() const;
  /** See \ref STORAGE */
  storage::StorageManager&        get_storage_manager() const;
  /** See \ref XCT */
  xct::XctManager&                get_xct_manager() const;
  /** See \ref RESTART */
  restart::RestartManager&        get_restart_manager() const;

 private:
  EnginePimpl* pimpl_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_HPP_
