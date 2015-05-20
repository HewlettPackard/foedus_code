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
#ifndef FOEDUS_SOC_SOC_MANAGER_HPP_
#define FOEDUS_SOC_SOC_MANAGER_HPP_

#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/module_type.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/soc/fwd.hpp"

namespace foedus {
namespace soc {
/**
 * @brief SOC manager, which maintains the shared resource across SOCs and, if this engine is
 * a master engine, manages child SOCs.
 * @ingroup SOC
 * @details
 * Initialization of this module is quite special because a master engine launches
 * child SOCs potentially as different processes. Also, to avoid leak of shared memory,
 * we take a bit complicated steps.
 *
 * @section SOC_INIT_ORDER Initialization Order
 * When the user instantiates an Engine object, the engine is initialized in the following order:
 *  \li Engine object constructor (which does almost nothing itself)
 *  \li (Optional) The user pre-registers user procedures (for emulated or fork children SOCs only).
 *  \li The user invokes Engine::initialize() of the master engine
 *  \li \b CAUTION-FROM-HERE. SOCManager allocates shared memory of required sizes.
 *  \li SOCManager in master engine launches child SOCs, waits for their progress.
 *  \li SOCManager in child SOC engines attach the shared memory and acknowledge it via
 * the shared memory.
 *  \li \b CAUTION-UNTIL-HERE. SOCManager in master engine determines if either all SOCManagers
 * attached the shared memory or there was some error. In either case, it marks the shared memory
 * for reclamation (shmctl(IPC_RMID)).
 *  \li Child SOCs move on to other modules' initialization.
 *  \li Master SOCManager receives the result of child SOC initilization, then does remaining
 * initialization, such as restart.
 *
 * Notice the annotated region. If there is an unexpected crash in master engine during this
 * period, we \b leak shared memory until reboot or manual reclamation (ipcrm command).
 * We must avoid it as much as possible. Be extra careful!
 */
class SocManager CXX11_FINAL : public virtual Initializable {
 public:
  friend class SocManagerPimpl;

  explicit SocManager(Engine* engine);
  ~SocManager();

  // Disable default constructors
  SocManager() CXX11_FUNC_DELETE;
  SocManager(const SocManager&) CXX11_FUNC_DELETE;
  SocManager& operator=(const SocManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /** Returns the shared memories maintained across SOCs */
  SharedMemoryRepo* get_shared_memory_repo();

  /**
   * @brief Wait for master engine to finish init/uninit the module.
   */
  ErrorStack  wait_for_master_module(bool init, ModuleType module);
  /**
   * @brief Wait for other engines to finish init/uninit the module.
   */
  ErrorStack  wait_for_children_module(bool init, ModuleType module);

  /** Announce fatal error state of this (either master or child) engine if possible. */
  void        report_engine_fatal_error();

  /**
   * @brief This should be called at the beginning of main() if the executable expects to be
   * spawned as SOC engines.
   * @details
   * This method detects if the process has been spawned from FOEDUS as an SOC engine.
   * If so, it starts running as an SOC engine and exits the process (via _exit()) when done.
   * This should be called as early as possible like following:
   * @code{.cpp}
   * int main(int argc, char** argv) {
   *   foedus::soc::SocManager::trap_spawned_soc_main();
   *   // user code ....
   *   return 0;
   * }
   * @endcode
   *
   * If the process has not been spawned as FOEDUS's SOC engine, this method does nothing
   * and immediately returns.
   */
  static void trap_spawned_soc_main();
  /**
   * @brief This version also receives user-defined procedures to be registered in this SOC.
   * @param[in] procedures user-defined procedures' name and function pointer \b in \b this
   * \b process.
   * @details
   * Example:
   * @code{.cpp}
   * ErrorStack my_proc(foedus::thread::Thread* context, ...) {
   *   ...
   *   return kRetOk;
   * }
   * int main(int argc, char** argv) {
   *   std::vector< foedus::proc::ProcAndName > procedures;
   *   procedures.push_back(foedus::proc::ProcAndName("my_proc", &my_proc));
   *   ...  // register more
   *   foedus::soc::SocManager::trap_spawned_soc_main(procedures);
   *   // user code ....
   *   return 0;
   * }
   * @endcode
   */
  static void trap_spawned_soc_main(const std::vector< proc::ProcAndName >& procedures);

 private:
  SocManagerPimpl *pimpl_;
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_MANAGER_HPP_
