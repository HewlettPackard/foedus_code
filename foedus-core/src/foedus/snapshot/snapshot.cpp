/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot.hpp"

#include <ostream>

namespace foedus {
namespace snapshot {
std::ostream& operator<<(std::ostream& o, const Snapshot& v) {
  o << "<Snapshot>"
    << "<id_>" << v.id_ << "</id_>"
    << "<base_epoch_>" << v.base_epoch_ << "</base_epoch_>"
    << "<valid_until_epoch_>" << v.valid_until_epoch_ << "</valid_until_epoch_>"
    << "</Snapshot>";
  return o;
}
}  // namespace snapshot
}  // namespace foedus
