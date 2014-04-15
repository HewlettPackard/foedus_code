/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct.hpp>
#include <foedus/xct/xct_access.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <ostream>
namespace foedus {
namespace xct {
Xct::Xct() {
    id_ = XctId();
    active_ = false;
    thread_ = nullptr;
    read_set_ = nullptr;
    read_set_size_ = 0;
    max_read_set_size_ = 0;
    write_set_ = nullptr;
    write_set_size_ = 0;
    max_write_set_size_ = 0;
}

void Xct::activate(thread::Thread* thread) {
    id_ = XctId();
    active_ = true;
    thread_ = thread;
    memory::NumaCoreMemory* core_memory = thread->get_thread_memory();
    read_set_ = core_memory->get_read_set_memory();
    read_set_size_ = 0;
    max_read_set_size_ = core_memory->get_read_set_size();
    write_set_ = core_memory->get_write_set_memory();
    write_set_size_ = 0;
    max_write_set_size_ = core_memory->get_write_set_size();
}


std::ostream& operator<<(std::ostream& o, const Xct& v) {
    o << "Xct: " << v.get_id() << ", read_set_size=" << v.get_read_set_size()
        << ", write_set_size=" << v.get_write_set_size();
    return o;
}
ErrorStack Xct::add_to_read_set(storage::Record* record) {
    if (read_set_size_ >= max_read_set_size_) {
        return ERROR_STACK(ERROR_CODE_XCT_READ_SET_OVERFLOW);
    }

    read_set_[read_set_size_].observed_owner_id_ = record->owner_id_;
    read_set_[read_set_size_].record_ = record;
    ++read_set_size_;
    return RET_OK;
}

ErrorStack Xct::add_to_write_set(storage::Record* record) {
    if (write_set_size_ >= max_write_set_size_) {
        return ERROR_STACK(ERROR_CODE_XCT_WRITE_SET_OVERFLOW);
    }

    write_set_[write_set_size_].observed_owner_id_ = record->owner_id_;
    write_set_[write_set_size_].record_ = record;
    ++read_set_size_;
    return RET_OK;
}

}  // namespace xct
}  // namespace foedus
