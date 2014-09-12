/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SOC_MANAGER_HPP_
#define FOEDUS_SOC_SOC_MANAGER_HPP_

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
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

 private:
  SocManagerPimpl *pimpl_;
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_MANAGER_HPP_
