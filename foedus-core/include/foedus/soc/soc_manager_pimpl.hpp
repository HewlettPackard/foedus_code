/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SOC_MANAGER_PIMPL_HPP_
#define FOEDUS_SOC_SOC_MANAGER_PIMPL_HPP_

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/soc/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

namespace foedus {
namespace soc {
/**
 * @brief Pimpl object of SocManager.
 * @ingroup SOC
 * @details
 * A private pimpl object for SocManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SocManagerPimpl final : public DefaultInitializable {
 public:
  SocManagerPimpl() = delete;
  explicit SocManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  Engine* const           engine_;
  SharedMemoryRepo        memory_repo_;
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_MANAGER_PIMPL_HPP_
