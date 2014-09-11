/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SOC_HOST_HPP_
#define FOEDUS_SOC_SOC_HOST_HPP_

#include <stdint.h>

#include "foedus/cxx11.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

namespace foedus {
namespace soc {

/**
 * @brief Represents a process of an SOC instance that hosts data and threads for the SOC.
 * @ingroup SOC
 * @details
 * Although this is a class in SOC package, this is the top object in terms of object hierarchy.
 * Each SOC, whether it begins as an emulated thread or forked/exec-ed process, instantiates
 * this object and keeps it until its end.
 */
class SocHost {
 public:
  SocHost() : emulated_(false), remote_(false), node_(0), engine_(CXX11_NULLPTR) {}

  /**
   * @brief Spawn a process for SOC of the given node.
   */
  ErrorStack spawn_host(
    const EngineOptions& options,
    uint16_t node,
    SharedMemoryRepo *memories);

  /**
   * @brief Spawn a thread for SOC of the given node.
   */
  void       emulate_host(
    const EngineOptions& options,
    uint16_t node,
    const SharedMemoryRepo& memories);

 private:
  /**
   * If this is true, the SOC is actually just a thread in the original process.
   * This is handy, robust and flexible, but has scalability limits.
   * If this is false, the SOC is actually a different process forked or exec-ed from the
   * original process.
   */
  bool              emulated_;

  /**
   * If this is true, the SOC process and the original process are in cache-incoherent
   * remote nodes. We support such a case only over RVMA.
   */
  bool              remote_;

  /**
   * NUMA node or ID of this SOC.
   */
  uint16_t          node_;

  EngineOptions     options_;

  /**
   * Each SOC has its own Engine object, allocated and deallocated individually.
   * The Engine instances do not share anything except the shared memory below.
   */
  Engine*           engine_;

  /** Shared memory of this FOEDUS instance */
  SharedMemoryRepo  memories_;

  void              emulated_handler();
};

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_HOST_HPP_
