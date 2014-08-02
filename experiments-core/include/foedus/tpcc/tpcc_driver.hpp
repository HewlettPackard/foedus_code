/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_DRIVER_HPP_
#define FOEDUS_TPCC_TPCC_DRIVER_HPP_

#include <stdint.h>

#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/tpcc/fwd.hpp"
#include "foedus/tpcc/tpcc.hpp"

namespace foedus {
namespace tpcc {

/**
 * @brief Main class for TPCC benchmark.
 */
class TpccDriver {
 public:
  explicit TpccDriver(Engine* engine) : engine_(engine) {
  }

  uint64_t run();

  std::vector<TpccClientTask*>& get_clients() { return clients_; }
  const TpccStorages&           get_storages() const { return storages_; }

 private:
  Engine* const engine_;

  std::vector<TpccClientTask*>  clients_;

  TpccStorages                  storages_;

  thread::Rendezvous            start_rendezvous_;
};

int driver_main(int argc, char **argv);

}  // namespace tpcc
}  // namespace foedus
#endif  // FOEDUS_TPCC_TPCC_DRIVER_HPP_
