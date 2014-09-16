/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/xct/xct.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "foedus/xct/xct_options.hpp"

namespace foedus {
namespace xct {
Xct::Xct(Engine* engine, thread::ThreadId thread_id) : engine_(engine), thread_id_(thread_id) {
  id_ = XctId();
  active_ = false;
  read_set_ = nullptr;
  read_set_size_ = 0;
  max_read_set_size_ = 0;
  write_set_ = nullptr;
  write_set_size_ = 0;
  max_write_set_size_ = 0;
  lock_free_write_set_ = nullptr;
  lock_free_write_set_size_ = 0;
  max_lock_free_write_set_size_ = 0;
  pointer_set_size_ = 0;
  page_version_set_size_ = 0;
  isolation_level_ = kSerializable;
  mcs_block_current_ = nullptr;
}

void Xct::initialize(memory::NumaCoreMemory* core_memory, uint32_t* mcs_block_current) {
  id_.set_epoch(engine_->get_savepoint_manager().get_savepoint_fast().get_current_epoch());
  id_.set_ordinal(0);  // ordinal 0 is possible only as a dummy "latest" XctId
  ASSERT_ND(id_.is_valid());
  memory::NumaCoreMemory:: SmallThreadLocalMemoryPieces pieces
    = core_memory->get_small_thread_local_memory_pieces();
  const XctOptions& xct_opt = engine_->get_options().xct_;
  read_set_ = reinterpret_cast<XctAccess*>(pieces.xct_read_access_memory_);
  read_set_size_ = 0;
  max_read_set_size_ = xct_opt.max_read_set_size_;
  write_set_ = reinterpret_cast<WriteXctAccess*>(pieces.xct_write_access_memory_);
  write_set_size_ = 0;
  max_write_set_size_ = xct_opt.max_write_set_size_;
  lock_free_write_set_ = reinterpret_cast<LockFreeWriteXctAccess*>(
    pieces.xct_lock_free_write_access_memory_);
  lock_free_write_set_size_ = 0;
  max_lock_free_write_set_size_ = xct_opt.max_lock_free_write_set_size_;
  pointer_set_ = reinterpret_cast<PointerAccess*>(pieces.xct_pointer_access_memory_);
  pointer_set_size_ = 0;
  page_version_set_ = reinterpret_cast<PageVersionAccess*>(pieces.xct_page_version_memory_);
  page_version_set_size_ = 0;
  mcs_block_current_ = mcs_block_current;
  *mcs_block_current_ = 0;
}

void Xct::issue_next_id(XctId max_xct_id, Epoch *epoch)  {
  ASSERT_ND(id_.is_valid());

  while (true) {
    // invariant 1: Larger than latest XctId of this thread.
    XctId new_id = id_;
    // invariant 2: Larger than every XctId of any record read or written by this transaction.
    new_id.store_max(max_xct_id);
    // invariant 3: in the epoch
    if (new_id.get_epoch().before(*epoch)) {
      new_id.set_epoch(*epoch);
      new_id.set_ordinal(0);
    }
    ASSERT_ND(new_id.get_epoch() == *epoch);

    // Now, is it possible to get an ordinal one larger than this one?
    if (UNLIKELY(new_id.get_ordinal() == 0xFFFFFFFFU)) {
      // oh, that's rare.
      LOG(WARNING) << "Reached the maximum ordinal in this epoch. Advancing current epoch"
        << " just for this reason. It's rare, but not an error.";
      engine_->get_xct_manager().advance_current_global_epoch();
      ASSERT_ND(epoch->before(engine_->get_xct_manager().get_current_global_epoch()));
      // we have already issued fence by now, so we can use nonatomic version.
      *epoch = engine_->get_xct_manager().get_current_global_epoch_weak();
      continue;  // try again with this epoch.
    }

    ASSERT_ND(new_id.get_ordinal() < 0xFFFFFFFFU);
    new_id.set_ordinal(new_id.get_ordinal() + 1U);
    remember_previous_xct_id(new_id);
    break;
  }
}

std::ostream& operator<<(std::ostream& o, const Xct& v) {
  o << "<Xct>"
    << "<active_>" << v.is_active() << "</active_>";
  if (v.is_active()) {
    o << "<id_>" << v.get_id() << "</id_>"
      << "<scheme_xct_>" << v.is_schema_xct() << "</scheme_xct_>"
      << "<read_set_size>" << v.get_read_set_size() << "</read_set_size>"
      << "<write_set_size>" << v.get_write_set_size() << "</write_set_size>"
      << "<pointer_set_size>" << v.get_pointer_set_size() << "</pointer_set_size>"
      << "<page_version_set_size>" << v.get_page_version_set_size() << "</page_version_set_size>"
      << "<lock_free_write_set_size>" << v.get_lock_free_write_set_size()
        << "</lock_free_write_set_size>";
  }
  o << "</Xct>";
  return o;
}

}  // namespace xct
}  // namespace foedus
