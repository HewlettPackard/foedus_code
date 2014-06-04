/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/xct/xct_inl.hpp>
#include <foedus/xct/xct_access.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/savepoint/savepoint_manager.hpp>
#include <foedus/savepoint/savepoint.hpp>
#include <glog/logging.h>
#include <ostream>
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
    isolation_level_ = SERIALIZABLE;
}

void Xct::initialize(thread::ThreadId thread_id, memory::NumaCoreMemory* core_memory) {
    id_.set_epoch(engine_->get_savepoint_manager().get_savepoint_fast().get_current_epoch());
    id_.set_thread_id(thread_id);
    id_.set_ordinal(0);  // ordinal 0 is possible only as a dummy "latest" XctId
    ASSERT_ND(id_.is_valid());
    read_set_ = core_memory->get_read_set_memory();
    read_set_size_ = 0;
    max_read_set_size_ = core_memory->get_read_set_size();
    write_set_ = core_memory->get_write_set_memory();
    write_set_size_ = 0;
    max_write_set_size_ = core_memory->get_write_set_size();
}

void Xct::issue_next_id(Epoch *epoch)  {
    ASSERT_ND(id_.is_valid());
    ASSERT_ND(id_.is_status_bits_off());

    while (true) {
        // invariant 1: Larger than latest XctId of this thread.
        XctId new_id = id_;
        // invariant 3: in the epoch
        if (new_id.get_epoch().before(*epoch)) {
            new_id.set_epoch(*epoch);
            new_id.set_ordinal(0);
        }
        ASSERT_ND(new_id.get_epoch() == *epoch);

        // invariant 2: Larger than every XctId of any record read or written by this transaction.
        for (uint32_t i = 0; i < read_set_size_; ++i) {
            new_id.store_max(read_set_[i].observed_owner_id_);
        }
        for (uint32_t i = 0; i < write_set_size_; ++i) {
            ASSERT_ND(write_set_[i].record_->owner_id_.is_keylocked());
            new_id.store_max(write_set_[i].record_->owner_id_);
        }

        // Now, is it possible to get an ordinal one larger than this one?
        if (UNLIKELY(new_id.get_ordinal() == 0xFFFFU)) {
            // oh, that's rare.
            LOG(WARNING) << "Reached the maximum ordinal in this epoch. Advancing current epoch"
                << " just for this reason. It's rare, but not an error.";
            engine_->get_xct_manager().advance_current_global_epoch();
            ASSERT_ND(epoch->before(engine_->get_xct_manager().get_current_global_epoch()));
            // we have already issued fence by now, so we can use nonatomic version.
            *epoch = engine_->get_xct_manager().get_current_global_epoch_nonatomic();
            continue;  // try again with this epoch.
        }

        ASSERT_ND(new_id.get_ordinal() < 0xFFFFU);
        new_id.set_ordinal(new_id.get_ordinal() + 1);
        new_id.set_thread_id(thread_id_);
        new_id.clear_status_bits();
        ASSERT_ND(id_.before(new_id));
        id_ = new_id;
        ASSERT_ND(id_.get_ordinal() > 0);
        ASSERT_ND(id_.is_valid());
        ASSERT_ND(id_.is_status_bits_off());
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
            << "<write_set_size>" << v.get_write_set_size() << "</write_set_size>";
    }
    o << "</Xct>";
    return o;
}

}  // namespace xct
}  // namespace foedus
