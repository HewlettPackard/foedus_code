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
#ifndef FOEDUS_ENGINE_OPTIONS_HPP_
#define FOEDUS_ENGINE_OPTIONS_HPP_

#include <iosfwd>

// rather than forward declarations of option classes for each module, we include them here.
// these are anyway very small header files, and demanding user code to include each of them
// won't fly. further, just holding instances, rather than pointers, makes (de)allocation simpler.
#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/cache/cache_options.hpp"
#include "foedus/debugging/debugging_options.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/log/log_options.hpp"
#include "foedus/memory/memory_options.hpp"
#include "foedus/proc/proc_options.hpp"
#include "foedus/restart/restart_options.hpp"
#include "foedus/savepoint/savepoint_options.hpp"
#include "foedus/snapshot/snapshot_options.hpp"
#include "foedus/soc/soc_options.hpp"
#include "foedus/storage/storage_options.hpp"
#include "foedus/thread/thread_options.hpp"
#include "foedus/xct/xct_options.hpp"
namespace foedus {
/**
 * @brief Set of option values given to the engine at start-up.
 * @ingroup ENGINE
 * @details
 * This object is a collection of engine-wide settings and settings for individual
 * modules (XxxOptions). To configure options, instantiate this class then modify
 * values in each sub-module's options.
 * When you start-up the database engine, you have to provide this object.
 *
 * @section INSTANTIATION Instantiating EngineOptions object
 * To instantiate an EngineOptions object, simply call the default constructor like:
 * @code{.cpp}
 * EngineOptions options;
 * @endcode
 * This sets default values to all settings. See each module's option classes for the description
 * of their default values.
 *
 * @section EXTERNALIZATION Externalization: Loading and Saving config values.
 * You can save and load this object to an XML file:
 * @code{.cpp}
 * EngineOptions options;
 * ... (change something in options)
 * if (options.save_to_file("/your/path/to/foedus_config.xml").is_error()) {
 *    // handle errors. It might be file permission issue or other file I/O issues.
 * }
 *
 * .... (after doing something else, possibly after restarting the program)
 * .... (or, maybe the user has edited the config file on text editor)
 * if (options.load_from_file("/your/path/to/foedus_config.xml").is_error()) {
 *    // handle errors. It might be file permission, corrupted XML files, etc.
 * }
 * @endcode
 */
struct EngineOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /**
   * Constructs option values with default values.
   */
  EngineOptions();
  EngineOptions(const EngineOptions& other);
  EngineOptions& operator=(const EngineOptions& other);

  /**
   * @param[out] shared_bytes Byte size of shared hugepage memory the engine will consume when
   * an engine starts up with these option values.
   * @param[out] local_bytes Byte size of node-local hugepage memory the engine will consume when
   * an engine starts up with these option values.
   */
  void calculate_required_memory(uint64_t* shared_bytes, uint64_t* local_bytes) const;

  /**
   * @brief Checks the machine environment and raises as many errors as possible before
   * the engine starts up.
   * @param[out] details_out We write out detailed messages (more like a documentation to help
   * users configure linux) to this stream, not to glog,
   * because glog might not be initialized at this point.
   * @details
   * There are several linux configurations the user must get right.
   * Some, not all, of misconfigurations can be detected beforehand by checking the machine
   * configuration and values of EngineOptions.
   * This method has no side-effect. It just checks the environment.
   * @attention This method can NOT detect all errors. For example, even if
   * the number of hugepages is apparantely sufficient at this point but other processes
   * might consume all of them by the time this engine allocates memory.
   * Still, this method is supposed to catch 99% of mis-configurations beforehand.
   */
  ErrorStack prescreen(std::ostream* details_out) const;

  /** Subroutine of prescreen to check ulimit values */
  void prescreen_ulimits(
    uint64_t required_total_safe_bytes,
    bool* has_any_error,
    std::ostream* details_out) const;

  /** Subroutine of prescreen to check sysctl values */
  void prescreen_sysctl(
    uint64_t required_shared_safe_bytes,
    bool* has_any_error,
    std::ostream* details_out) const;

  /**
   * A utility method to read /proc/ value in linux.
   * @param[in] path file path representing a config value, eg "/proc/sys/kernel/shmall"
   * @param[out] details_out if there is any concern, we output some message to this stream.
   * @returns an integer value stored in /proc/...
   */
  static uint64_t read_int_from_proc_fs(const char* path, std::ostream* details_out);
  /**
   * A utility method to check how much hugepage memory is currently available.
   * @param[out] details_out if there is any concern, we output some message to this stream.
   * @returns byte size of available memory backed by non-transparent hugepages.
   */
  static uint64_t get_available_hugepage_memory(std::ostream* details_out);

  // options for each module
  cache::CacheOptions         cache_;
  debugging::DebuggingOptions debugging_;
  log::LogOptions             log_;
  memory::MemoryOptions       memory_;
  proc::ProcOptions           proc_;
  restart::RestartOptions     restart_;
  savepoint::SavepointOptions savepoint_;
  snapshot::SnapshotOptions   snapshot_;
  soc::SocOptions             soc_;
  storage::StorageOptions     storage_;
  thread::ThreadOptions       thread_;
  xct::XctOptions             xct_;

  EXTERNALIZABLE(EngineOptions);
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_OPTIONS_HPP_
