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

 private:
  SocManagerPimpl *pimpl_;
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_MANAGER_HPP_
