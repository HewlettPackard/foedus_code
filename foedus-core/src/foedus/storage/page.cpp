/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/page.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {

std::ostream& operator<<(std::ostream& o, const PageVersionStatus& v) {
  o << "<PageVersionStatus><flags>"
    << (v.is_moved() ? "M" : " ")
    << (v.is_retired() ? "R" : " ")
    << "</flags><ver>" << v.get_version_counter() << "</ver>"
    << "</PageVersionStatus>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const PageVersion& v) {
  o << "<PageVersion><locked>"
    << (v.is_locked() ? "L" : " ")
    << (v.is_moved() ? "M" : " ")
    << (v.is_retired() ? "R" : " ")
    << "</locked>" << v.status_
    << "</PageVersion>";
  return o;
}
PageVersionLockScope::PageVersionLockScope(
  thread::Thread* context,
  PageVersion* version,
  bool initial_lock) {
  context_ = context;
  version_ = version;
  changed_ = false;
  released_ = false;
  if (initial_lock) {
    block_ = context->mcs_initial_lock(&version->lock_);
  } else {
    block_ = context->mcs_acquire_lock(&version->lock_);
  }
}

void PageVersionLockScope::release() {
  if (!released_) {
    if (changed_) {
      version_->increment_version_counter();
    }
    context_->mcs_release_lock(&version_->lock_, block_);
    released_ = true;
  }
}


}  // namespace storage
}  // namespace foedus
