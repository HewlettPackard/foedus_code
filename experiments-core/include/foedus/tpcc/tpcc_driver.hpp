/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_DRIVER_HPP_
#define FOEDUS_TPCC_TPCC_DRIVER_HPP_

#include <stdint.h>

#include <iosfwd>
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
  struct Result {
    Result()
      : processed_(0),
        user_requested_aborts_(0),
        race_aborts_(0),
        largereadset_aborts_(0),
        unexpected_aborts_(0) {}
    uint64_t processed_;
    uint64_t user_requested_aborts_;
    uint64_t race_aborts_;
    uint64_t largereadset_aborts_;
    uint64_t unexpected_aborts_;
    friend std::ostream& operator<<(std::ostream& o, const Result& v);
  };
  explicit TpccDriver(Engine* engine) : engine_(engine) {
  }

  Result run();

  std::vector<TpccClientTask*>& get_clients() { return clients_; }
  const TpccStorages&           get_storages() const { return storages_; }

 private:
  Engine* const engine_;

  std::vector<TpccClientTask*>  clients_;

  /** inclusive beginning of responsible wid. index=thread ordinal */
  std::vector<Wid>              from_wids_;
  /** exclusive end of responsible wid. index=thread ordinal */
  std::vector<Wid>              to_wids_;

  /** inclusive beginning of responsible iid. index=thread ordinal */
  std::vector<Iid>              from_iids_;
  /** exclusive end of responsible iid. index=thread ordinal */
  std::vector<Iid>              to_iids_;

  TpccStorages                  storages_;

  thread::Rendezvous            start_rendezvous_;

  void assign_wids();
  void assign_iids();
};

int driver_main(int argc, char **argv);

}  // namespace tpcc
}  // namespace foedus
#endif  // FOEDUS_TPCC_TPCC_DRIVER_HPP_
