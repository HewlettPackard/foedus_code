/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/soc_host.hpp"

#include <thread>

#include "foedus/assert_nd.hpp"

namespace foedus {
namespace soc {

ErrorStack SocHost::spawn_host(
  const EngineOptions& options,
  uint16_t node,
  SharedMemoryRepo* memories) {

}

void SocHost::emulate_host(
  const EngineOptions& options,
  uint16_t node,
  const SharedMemoryRepo& memories) {
  emulated_ = true;
  remote_ = false;
  node_ = node;
  memories_.steal_shared(memories_);
  options_ = options;
  std::thread host_thread(emulated_handler);
  host_thread.detach();
}

void SocHost::emulated_handler() {
  engine_ = new Engine(options_);
  engine_->initialize();
}

}  // namespace soc
}  // namespace foedus
