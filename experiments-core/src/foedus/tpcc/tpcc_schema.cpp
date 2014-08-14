/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_schema.hpp"

#include <cstring>

namespace foedus {
namespace tpcc {

TpccStorages::TpccStorages() {
  std::memset(this, 0, sizeof(TpccStorages));
}


}  // namespace tpcc
}  // namespace foedus
