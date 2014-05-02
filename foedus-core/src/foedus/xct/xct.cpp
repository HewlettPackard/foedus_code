/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_inl.hpp>
#include <foedus/xct/xct_access.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <atomic>
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


Epoch Xct::get_in_commit_log_epoch() const {
    std::atomic_thread_fence(std::memory_order_acquire);
    return in_commit_log_epoch_;
}

Xct::InCommitLogEpochGuard::~InCommitLogEpochGuard() {
    // flush cachelines (which might contain the change on ThreadLogBuffer#offset_committed_)
    // BEFORE update to in_commit_log_epoch_. This is to satisfy the first requirement:
    // ("When this returns 0, this transaction will not publish any more log without getting
    // recent epoch").
    // Without this fence, logger can potentially miss the log that has been just published
    // with the old epoch.
    std::atomic_thread_fence(std::memory_order_release);
    xct_->in_commit_log_epoch_ = Epoch(0);
    // We can also call another memory_order_release here to immediately publish it,
    // but it's anyway rare. The spinning logger will eventually get the update, so no need.
    // In non-TSO architecture, this also saves some overhead in critical path.
}

}  // namespace xct
}  // namespace foedus
