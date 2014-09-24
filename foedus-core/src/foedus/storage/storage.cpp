/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage.hpp"

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/storage/storage_manager.hpp"

namespace foedus {
namespace storage {
std::ostream& operator<<(std::ostream& o, const Storage& v) {
  v.describe(&o);
  return o;
}

}  // namespace storage
}  // namespace foedus
