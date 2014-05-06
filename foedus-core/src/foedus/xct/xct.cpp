/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_inl.hpp>
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
    read_set_ = nullptr;
    read_set_size_ = 0;
    max_read_set_size_ = 0;
    write_set_ = nullptr;
    write_set_size_ = 0;
    max_write_set_size_ = 0;
    isolation_level_ = SERIALIZABLE;
}

void Xct::initialize(thread::ThreadId thread_id, memory::NumaCoreMemory* core_memory) {
    id_.thread_id_ = thread_id;
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

}  // namespace xct
}  // namespace foedus
