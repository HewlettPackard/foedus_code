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
#include <foedus/savepoint/savepoint.hpp>
#include <foedus/savepoint/savepoint_manager.hpp>
#include <glog/logging.h>
#include <ostream>
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

    const savepoint::Savepoint &savepoint = engine_->get_savepoint_manager().get_savepoint_fast();
    last_epoch_ = savepoint.get_current_epoch();
    logger_epoch_ = savepoint.get_current_epoch();
    offset_head_ = 0;
    offset_durable_ = 0;
    offset_committed_ = 0;
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
        && offset_committed_ < buffer_size_
        && offset_tail_ < buffer_size_);
    // because of wrap around, *at most* one of them does not hold
    int violation_count = 0;
    if (offset_head_ > offset_durable_) {
        ++violation_count;
    }
    if (offset_durable_ > offset_committed_) {
        ++violation_count;
    }
    if (offset_committed_ > offset_tail_) {
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

void ThreadLogBuffer::on_new_epoch_observed(Epoch commit_epoch) {
    ASSERT_ND(commit_epoch > last_epoch_);
    VLOG(0) << "Thread-" << thread_id_ << " wrote out the first log entry in epoch-" << commit_epoch
        << " at offset " << offset_committed_ << ". old epoch=" << last_epoch_;
    DVLOG(0) << "Before: " << *this;
    ThreadEpockMark mark;
    mark.new_epoch_ = commit_epoch;
    mark.old_epoch_ = last_epoch_;
    mark.offset_epoch_begin_ = offset_committed_;
    last_epoch_ = commit_epoch;

    if (logger_epoch_ > mark.old_epoch_) {
        LOG(INFO) << "Thread-" << thread_id_ << " realized that its last_epoch_ is older"
            " than logger_epoch_, most likely this thread has been idle for a while";
        // Check if that's the case..
        ASSERT_ND(offset_committed_ == offset_durable_);
    }

    {
        std::lock_guard<std::mutex> guard(thread_epoch_marks_mutex_);
        thread_epoch_marks_.emplace_back(mark);
    }
    DVLOG(0) << "After: " << *this;
}

void ThreadLogBuffer::crash_stale_commit_epoch(Epoch commit_epoch) {
    LOG(FATAL) << "Received a log-publication request with commit_epoch=" << commit_epoch
        << ", which is older than logger_epoch_=" << logger_epoch_ << ", this is a BUG!"
        << std::endl << " Buffer=" << *this;
}

bool ThreadLogBuffer::consume_epoch_mark() {
    // this is called only when the logger has nothing to flush for the logger_epoch_
    ASSERT_ND(logger_epoch_open_ended_ || logger_epoch_ends_ == offset_durable_);
    ThreadEpockMark mark;
    bool last_mark;
    uint64_t next_mark_begin = 0;  // set when last_mark is false
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
void ThreadLogBuffer::consume_epoch_mark_as_many() {
    while (logger_epoch_open_ended_ || logger_epoch_ends_ == offset_durable_) {
        bool consumed = consume_epoch_mark();
        if (!consumed) {
            break;
        }
    }
}


std::ostream& operator<<(std::ostream& o, const ThreadLogBuffer& v) {
    o << "<ThreadLogBuffer>";
    o << "<thread_id_>" << v.thread_id_ << "</thread_id_>";
    o << "<buffer_size_>" << v.buffer_size_ << "</buffer_size_>";
    o << "<offset_head_>" << v.offset_head_ << "</offset_head_>";
    o << "<offset_durable_>" << v.offset_durable_ << "</offset_durable_>";
    o << "<offset_committed_>" << v.offset_committed_ << "</offset_committed_>";
    o << "<offset_tail_>" << v.offset_tail_ << "</offset_tail_>";
    o << "<last_epoch_>" << v.last_epoch_ << "</last_epoch_>";
    o << "<logger_epoch_>" << v.logger_epoch_ << "</logger_epoch_>";
    o << "<logger_epoch_open_ended_>" << v.logger_epoch_open_ended_
        << "</logger_epoch_open_ended_>";
    o << "<logger_epoch_ends_>" << v.logger_epoch_ends_ << "</logger_epoch_ends_>";
    o << "<thread_epoch_marks_>";
    for (auto mark : v.thread_epoch_marks_) {
        o << mark;
    }
    o << "</thread_epoch_marks_>";
    o << "</ThreadLogBuffer>";
    return o;
}

std::ostream& operator<<(std::ostream& o, const ThreadLogBuffer::ThreadEpockMark& v) {
    o << "<ThreadEpockMark>";
    o << "<old_epoch_>" << v.old_epoch_ << "</old_epoch_>";
    o << "<new_epoch_>" << v.new_epoch_ << "</new_epoch_>";
    o << "<offset_epoch_begin_>" << v.offset_epoch_begin_ << "</offset_epoch_begin_>";
    o << "</ThreadEpockMark>";
    return o;
}

}  // namespace log
}  // namespace foedus
