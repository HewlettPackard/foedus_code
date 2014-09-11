/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_
#define FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_

#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/proc/fwd.hpp"
#include "foedus/proc/proc_id.hpp"

namespace foedus {
namespace proc {
/**
 * @brief Pimpl object of ProcManager.
 * @ingroup PROC
 * @details
 * A private pimpl object for ProcManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ProcManagerPimpl final : public DefaultInitializable {
 public:
  /** Calculates required byte size of shared memory for this module. */
  static uint64_t get_required_shared_memory_size(const EngineOptions& options);

  ProcManagerPimpl() = delete;
  explicit ProcManagerPimpl(Engine* engine) : engine_(engine) {}

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  pre_register(ProcAndName proc_and_name);
  ErrorStack  local_register(ProcAndName proc_and_name);
  ErrorStack  emulated_register(ProcAndName proc_and_name);

  Engine* const               engine_;
  std::vector< ProcAndName >  pre_registered_procs_;
};
}  // namespace proc
}  // namespace foedus
#endif  // FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_
