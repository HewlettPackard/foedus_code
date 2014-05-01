/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/log/common_log_types.hpp>
#include <foedus/engine.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/assert_nd.hpp>
#include <glog/logging.h>
#include <list>
namespace foedus {
namespace log {
ThreadLogBuffer::ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id)
    : engine_(engine), thread_id_(thread_id) {
    buffer_ = nullptr;
    buffer_size_ = 0;
}
ErrorStack ThreadLogBuffer::initialize_once() {
    memory::NumaCoreMemory *memory = engine_->get_memory_manager().get_core_memory(thread_id_);
    buffer_memory_ = memory->get_log_buffer_memory();
    buffer_ = buffer_memory_.get_block();
    buffer_size_ = buffer_memory_.size();
    buffer_size_safe_ = buffer_size_ - 64;
    durable_epoch_ = xct::Epoch();
    current_epoch_ = xct::Epoch();
    logger_epoch_ = xct::Epoch();
    offset_head_ = 0;
    offset_durable_ = 0;
    offset_current_xct_begin_ = 0;
    offset_tail_ = 0;
    return RET_OK;
}

ErrorStack ThreadLogBuffer::uninitialize_once() {
    buffer_memory_.clear();
    buffer_ = nullptr;
    thread_epoch_marks_.clear();
    return RET_OK;
}

void ThreadLogBuffer::assert_consistent_offsets() const {
    ASSERT_ND(offset_head_ < buffer_size_
        && offset_durable_ < buffer_size_
        && offset_current_xct_begin_ < buffer_size_
        && offset_tail_ < buffer_size_);
    // because of wrap around, *at most* one of them does not hold
    int violation_count = 0;
    if (offset_head_ > offset_durable_) {
        ++violation_count;
    }
    if (offset_durable_ > offset_current_xct_begin_) {
        ++violation_count;
    }
    if (offset_current_xct_begin_ > offset_tail_) {
        ++violation_count;
    }
    ASSERT_ND(violation_count <= 1);
}

void ThreadLogBuffer::wait_for_space(uint16_t required_space) {
    // TODO(Hideaki) implement
    LOG(INFO) << "Thread-" << thread_id_ << " waiting for space to write logs..";
    while(true);
}

void ThreadLogBuffer::fillup_tail() {
    uint64_t len = buffer_size_ - offset_tail_;
    if (distance(buffer_size_, offset_tail_, offset_head_) + len >= buffer_size_safe_) {
        wait_for_space(len);
    }
    ASSERT_ND(distance(buffer_size_, offset_tail_, offset_head_) + len < buffer_size_safe_);
    FillerLogType *filler = reinterpret_cast<FillerLogType*>(buffer_ + offset_tail_);
    filler->init(len);
    advance(buffer_size_, &offset_tail_, len);
    ASSERT_ND(offset_tail_ == 0);
}

void ThreadLogBuffer::add_thread_epock_mark(const xct::Epoch &commit_epoch) {
    ASSERT_ND(commit_epoch > current_epoch_);
    VLOG(0) << "Thread-" << thread_id_ << " wrote out the first log entry in epoch-" << commit_epoch
        << " at offset " << offset_current_xct_begin_ << ". old epoch=" << current_epoch_;
    ThreadEpockMark mark;
    mark.new_epoch_ = commit_epoch;
    mark.old_epoch_ = current_epoch_;
    mark.offset_epoch_begin_ = offset_current_xct_begin_;
    current_epoch_ = commit_epoch;

    {
        std::lock_guard<std::mutex> guard(thread_epoch_marks_mutex_);
        thread_epoch_marks_.emplace_back(mark);
    }
}

bool ThreadLogBuffer::consume_epoch_mark() {
    // this is called only when the logger has nothing to flush for the logger_epoch_
    ASSERT_ND(!logger_epoch_.is_valid() || logger_epoch_open_ended_
        || logger_epoch_ends_ != offset_durable_);
    ThreadEpockMark mark;
    bool last_mark;
    uint64_t next_mark_begin = 0;  // only when last_mark is true
    {
        std::lock_guard<std::mutex> guard(thread_epoch_marks_mutex_);
        if (thread_epoch_marks_.empty()) {
            return false;
        }

        mark = thread_epoch_marks_.front();
        thread_epoch_marks_.pop_front();
        last_mark = thread_epoch_marks_.empty();
        if (!last_mark) {
            next_mark_begin = thread_epoch_marks_.front().offset_epoch_begin_;
        }
    }

    if (offset_durable_ != mark.offset_epoch_begin_) {
        LOG(FATAL) << "WHAT? Thread-" << thread_id_ << "'s log buffer is inconsistent."
            << " offset_durable_=" << offset_durable_ << ". but next marker begins at "
                << mark.offset_epoch_begin_;
    }

    logger_epoch_ = mark.new_epoch_;
    logger_epoch_open_ended_ = last_mark;
    logger_epoch_ends_ = next_mark_begin;
    return true;
}

}  // namespace log
}  // namespace foedus
