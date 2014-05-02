/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/log/common_log_types.hpp>
#include <foedus/log/logger_impl.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/savepoint/savepoint.hpp>
#include <foedus/savepoint/savepoint_manager.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/assert_nd.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <foedus/xct/xct.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <string>
#include <sstream>
#include <thread>
#include <vector>
namespace foedus {
namespace log {
fs::Path Logger::construct_suffixed_log_path(LogFileOrdinal ordinal) const {
    std::stringstream path_str;
    path_str << log_path_.string() << "." << ordinal;
    return fs::Path(path_str.str());
}

ErrorStack Logger::initialize_once() {
    // clear all variables
    current_file_ = nullptr;
    numa_node_ = static_cast<int>(thread::decompose_numa_node(assigned_thread_ids_[0]));
    durable_epoch_ = xct::Epoch();
    current_epoch_ = xct::Epoch();
    LOG(INFO) << "Initializing Logger-" << id_ << ". assigned " << assigned_thread_ids_.size()
        << " threads, starting from " << assigned_thread_ids_[0] << ", numa_node_="
        << static_cast<int>(numa_node_);

    // Initialize the values from the latest savepoint.
    // this is during initialization. no race.
    const savepoint::Savepoint &savepoint = engine_->get_savepoint_manager().get_savepoint_fast();
    oldest_ordinal_ = savepoint.oldest_log_files_[id_];
    current_ordinal_ = savepoint.current_log_files_[id_];
    current_file_length_ = savepoint.current_log_files_offset_durable_[id_];
    oldest_file_offset_begin_ = savepoint.oldest_log_files_offset_begin_[id_];
    current_file_path_ = construct_suffixed_log_path(current_ordinal_);
    // open the log file
    current_file_ = new fs::DirectIoFile(current_file_path_,
                                         engine_->get_options().log_.emulation_);
    CHECK_ERROR(current_file_->open(false, true, true, savepoint.empty()));
    uint64_t current_length = fs::file_size(current_file_path_);
    if (current_file_length_ < fs::file_size(current_file_path_)) {
        // there are non-durable regions as an incomplete remnant of previous execution.
        // probably there was a crash. in this case, we discard the non-durable regions.
        LOG(ERROR) << "Logger-" << id_ << "'s log file has a non-durable region. Probably there"
            << " was a crash. Will truncate it to " << current_file_length_
            << " from " << current_length;
        CHECK_ERROR(current_file_->truncate(current_file_length_, true));  // also sync right now
    }

    // which threads are assigned to me?
    for (auto thread_id : assigned_thread_ids_) {
        assigned_threads_.push_back(
            engine_->get_thread_pool().get_pimpl()->get_thread(thread_id));
    }

    // grab a buffer to do file I/O
    CHECK_ERROR(engine_->get_memory_manager().get_node_memory(numa_node_)->allocate_numa_memory(
        FillerLogType::LOG_WRITE_UNIT_SIZE, &fill_buffer_));
    LOG(INFO) << "Logger-" << id_ << " grabbed a I/O buffer. size=" << fill_buffer_.get_size();

    // log file and buffer prepared. let's launch the logger thread
    logger_thread_.initialize("Logger-", id_,
                    std::thread(&Logger::handle_logger, this), std::chrono::milliseconds(10));
    return RET_OK;
}

ErrorStack Logger::uninitialize_once() {
    LOG(INFO) << "Uninitializing Logger-" << id_ << ".";
    ErrorStackBatch batch;
    logger_thread_.stop();
    if (current_file_) {
        current_file_->close();
        delete current_file_;
        current_file_ = nullptr;
    }
    fill_buffer_.release_block();
    return RET_OK;
}

void Logger::handle_logger() {
    LOG(INFO) << "Logger-" << id_ << " started. pin on NUMA node-" << static_cast<int>(numa_node_);
    ::numa_run_on_node(numa_node_);
    while (!logger_thread_.sleep()) {
        const int MAX_ITERATIONS = 100;
        int iterations = 0;
        while (!logger_thread_.is_stop_requested()) {
            bool more_log_to_process = false;
            COERCE_ERROR(update_durable_epoch());
            for (thread::Thread* the_thread : assigned_threads_) {
                if (logger_thread_.is_stop_requested()) {
                    break;
                }

                // we FIRST take offset_committed with memory fence for the reason below.
                ThreadLogBuffer& buffer = the_thread->get_thread_log_buffer();
                std::atomic_thread_fence(std::memory_order_acquire);
                const uint64_t offset_committed = buffer.get_offset_committed();
                if (offset_committed == buffer.get_offset_durable()) {
                    VLOG(1) << "Thread-" << the_thread->get_thread_id() << " has no log to flush.";
                    continue;
                }
                VLOG(0) << "Thread-" << the_thread->get_thread_id() << " has "
                    << (offset_committed - buffer.get_offset_durable()) << " bytes logs to flush.";
                std::atomic_thread_fence(std::memory_order_acquire);

                // (if we need to) we consume epoch mark AFTER the fence. Thus, we don't miss a
                // case where the thread adds a new epoch mark after we read offset_committed.
                buffer.consume_epoch_mark_as_many();

                if (buffer.logger_epoch_open_ended_
                    && buffer.logger_epoch_ends_ == buffer.offset_durable_) {
                    // Then, the buffer is empty and maybe the worker thread is idle
                    continue;
                }

                if (buffer.logger_epoch_ < current_epoch_) {
                    // this can happen if the worker thread has been idle for a while.
                    // however, then there must be no log at least as of the fence above.
                    if (offset_committed != buffer.offset_durable_) {
                        LOG(FATAL) << "WHAT? Did I miss something? current_epoch_="
                            << current_epoch_ << ", buffer=" << buffer;
                    }
                    continue;
                }
                if (buffer.logger_epoch_ > current_epoch_) {
                    // then skip it for now. we must finish the current epoch first.
                    VLOG(0) << "Skipped " << the_thread->get_thread_id() << "'s log. too recent.";
                    more_log_to_process = true;
                    continue;
                } else {
                    // okay, let's write out logs in this buffer
                    uint64_t upto_offset;
                    if (buffer.logger_epoch_open_ended_) {
                        // then, we write out upto offset_committed_. however, consider the case:
                        // 1) buffer has no mark (open ended) durable=10, committed=20, ep=3.
                        // 2) this logger comes by with current_epoch=3. Sees no mark in buffer.
                        // 3) buffer receives new log in the meantime, ep=4, new mark added,
                        //   and committed is now 30.
                        // 4) logger "okay, I will flush out all logs up to committed(30)".
                        // 5) logger writes out all logs up to 30, as ep=3.
                        // To prevent this case, we first read offset_committed, take fence, then
                        // check epoch mark.
                        upto_offset = offset_committed;  // use the already-copied value
                    } else {
                        upto_offset = buffer.logger_epoch_ends_;
                    }

                    COERCE_ERROR(write_log(&buffer, upto_offset));
                }
            }

            COERCE_ERROR(update_durable_epoch());

            if (!more_log_to_process) {
                break;
            }
            if (++iterations >= MAX_ITERATIONS) {
                LOG(WARNING) << "Logger-" << id_ << " has been working without sleep for long time"
                    << ". Either too few loggers or potentially a bug??";
                break;
            } else {
                LOG(INFO) << "Logger-" << id_ << " has more task. keep working. " << iterations;
            }
        }
    }
    LOG(INFO) << "Logger-" << id_ << " ended.";
}

ErrorStack Logger::update_durable_epoch() {
    std::atomic_thread_fence(std::memory_order_acquire);  // not necessary. just to get latest info.
    const xct::Epoch global_epoch = engine_->get_xct_manager().get_current_global_epoch();
    std::atomic_thread_fence(std::memory_order_acquire);  // necessary. following is AFTER this.

    VLOG(1) << "Logger-" << id_ << " update_durable_epoch(). durable_epoch_=" << durable_epoch_
        << ", global_epoch=" << global_epoch;

    xct::Epoch min_durable_epoch = global_epoch.one_less();
    for (thread::Thread* the_thread : assigned_threads_) {
        ThreadLogBuffer& buffer = the_thread->get_thread_log_buffer();
        DVLOG(1) << buffer;
        if (buffer.logger_epoch_ > min_durable_epoch) {
            VLOG(1) << "Thread-" << the_thread->get_thread_id() << " at least durable up to "
                << min_durable_epoch;
            continue;
        }

        // in the worst case, buffer is durable up to only logger_epoch_ - 1. let's check.
        if (!buffer.logger_epoch_open_ended_) {
            // then easier. we do know up to where we have to flush log to make it durable.
            if (buffer.logger_epoch_open_ended_ != buffer.offset_durable_) {
                // there are logs we have to flush, so surely logger_epoch_ - 1.
                VLOG(1) << "Thread-" << the_thread->get_thread_id() << " more logs in this ep";
                min_durable_epoch.store_min(buffer.logger_epoch_.one_less());
            } else {
                // Then no chance the thread adds more log to this epoch, good!
                VLOG(1) << "Thread-" << the_thread->get_thread_id() << " no more logs in this ep";
                min_durable_epoch.store_min(buffer.logger_epoch_);
                buffer.consume_epoch_mark();  // we can probably update buffer.logger_epoch_, too.
            }
        } else {
            VLOG(1) << "Thread-" << the_thread->get_thread_id() << " open-ended in this ep";
            // we are not sure whether we flushed everything in this epoch.
            xct::Epoch epoch_before = buffer.logger_epoch_;
            if (buffer.consume_epoch_mark()) {
                VLOG(1) << "Thread-" << the_thread->get_thread_id() << " consumed epoch mark!";
                ASSERT_ND(buffer.logger_epoch_ > epoch_before);
                min_durable_epoch.store_min(epoch_before);
            } else {
                // mm, really open ended, but the thread might not be in commit phase.
                xct::Xct& xct = the_thread->get_current_xct();
                xct::Epoch in_commit_epoch = xct.get_in_commit_log_epoch();
                // See get_in_commit_log_epoch() about the protocol of in_commit_log_epoch
                if (!in_commit_epoch.is_valid() || in_commit_epoch > epoch_before) {
                    VLOG(1) << "Thread-" << the_thread->get_thread_id() << " not in a racy state!";
                    min_durable_epoch.store_min(epoch_before);
                } else {
                    ASSERT_ND(in_commit_epoch == epoch_before);
                    // This is rare. worth logging in details. In this case, we spin on it.
                    // This state is guaranteed to quickly end.
                    // By doing this, handle_logger() can use this method for waiting.
                    LOG(INFO) << "Thread-" << the_thread->get_thread_id() << " is now publishing"
                        " logs that might be in epoch-" << epoch_before;
                    DLOG(INFO) << buffer;
                    while (in_commit_epoch.is_valid() && in_commit_epoch <= epoch_before) {
                        in_commit_epoch = xct.get_in_commit_log_epoch();
                    }
                    LOG(INFO) << "Thread-" << the_thread->get_thread_id() << " is done with the log"
                        ". " << epoch_before;
                    DLOG(INFO) << buffer;

                    // Just logging. we conservatively use one_less because the transaction
                    // probably produced logs in the epoch. We have to flush them first.
                    min_durable_epoch.store_min(epoch_before.one_less());
                }
            }
        }
    }

    ASSERT_ND(min_durable_epoch >= durable_epoch_);
    if (min_durable_epoch > durable_epoch_) {
        LOG(INFO) << "Logger-" << id_ << " updates durable_epoch_ from " << durable_epoch_
            << " to " << min_durable_epoch;
        if (!fs::fsync(current_file_path_, true)) {
            return ERROR_STACK(ERROR_CODE_FS_SYNC_FAILED);
        }
        LOG(INFO) << "Logger-" << id_ << " fsynced the current file and its folder";
        std::atomic_thread_fence(std::memory_order_release);  // announce it only AFTER above
        durable_epoch_ = min_durable_epoch;  // announce it!
        std::atomic_thread_fence(std::memory_order_release);  // so that log manager sees it asap

        // Also, update current_epoch_.
        CHECK_ERROR(switch_current_epoch(durable_epoch_.one_more()));
    } else {
        VLOG(1) << "Logger-" << id_ << " couldn't update durable_epoch_";
    }

    return RET_OK;
}

ErrorStack Logger::switch_current_epoch(const xct::Epoch& new_epoch) {
    ASSERT_ND(current_epoch_ < new_epoch);
    VLOG(0) << "Logger-" << id_ << " advances its current_epoch from " << current_epoch_
        << " to " << new_epoch;

    // Use fill buffer to write out the epoch mark log
    char* buf = reinterpret_cast<char*>(fill_buffer_.get_block());
    EpochMarkerLogType* epoch_marker = reinterpret_cast<EpochMarkerLogType*>(buf);
    epoch_marker->header_.storage_id_ = 0;
    epoch_marker->header_.log_length_ = sizeof(EpochMarkerLogType);
    epoch_marker->header_.log_type_code_ = get_log_code<EpochMarkerLogType>();
    epoch_marker->new_epoch_ = new_epoch;
    epoch_marker->old_epoch_ = current_epoch_;

    // Fill it up to 4kb and write. A bit wasteful, but happens only once per epoch
    FillerLogType* filler_log = reinterpret_cast<FillerLogType*>(buf + sizeof(EpochMarkerLogType));
    filler_log->init(fill_buffer_.get_size() - sizeof(EpochMarkerLogType));

    CHECK_ERROR_CODE(current_file_->write(fill_buffer_.get_size(), fill_buffer_));
    current_epoch_ = new_epoch;
    return RET_OK;
}

ErrorStack Logger::switch_file_if_required() {
    if (current_file_length_ < (engine_->get_options().log_.log_file_size_mb_ << 20)) {
        return RET_OK;
    }

    VLOG(0) << "Logger-" << id_ << " moving on to next file. current_ordinal_=" << current_ordinal_;

    // Close the current one. Immediately call fsync on it AND the parent folder.
    current_file_->close();
    delete current_file_;
    current_file_ = nullptr;
    if (!fs::fsync(current_file_path_, true)) {
        return ERROR_STACK(ERROR_CODE_FS_SYNC_FAILED);
    }

    current_file_path_ = construct_suffixed_log_path(++current_ordinal_);
    VLOG(0) << "Logger-" << id_ << " next file=" << current_file_path_;
    current_file_ = new fs::DirectIoFile(current_file_path_,
                                            engine_->get_options().log_.emulation_);
    CHECK_ERROR(current_file_->open(false, true, true, true));
    current_file_length_ = 0;
    return RET_OK;
}

ErrorStack Logger::write_log(ThreadLogBuffer* buffer, uint64_t upto_offset) {
    CHECK_ERROR(switch_file_if_required());
    const uint64_t from_offset = buffer->get_offset_durable();
    const uint64_t SECTOR_SIZE = FillerLogType::LOG_WRITE_UNIT_SIZE;
    ASSERT_ND(from_offset != upto_offset);
    if (from_offset > upto_offset) {
        // this means wrap-around in the buffer
        // let's write up to the end of the circular buffer, then from the beginning.
        VLOG(0) << "Writing out Thread-" << buffer->get_thread_id() << "'s log with wrap around."
            << " from_offset=" << from_offset << ", upto_offset=" << upto_offset;

        // we can simply write out logs upto the end without worrying about the case where a log
        // entry spans the end of circular buffer. Because we avoid that in ThreadLogBuffer.
        // (see reserve_new_log()). So, we can separately handle the two writes by calling itself
        // again, which adds padding if they need.
        CHECK_ERROR(write_log(buffer, buffer->buffer_size_));
        ASSERT_ND(buffer->get_offset_durable() == 0);
        CHECK_ERROR(write_log(buffer, upto_offset));
        ASSERT_ND(buffer->get_offset_durable() == upto_offset);
        return RET_OK;
    } else {
        const uint64_t write_size = upto_offset - from_offset;
        const uint64_t padded_write_size = assorted::align< uint64_t, SECTOR_SIZE >(write_size);
        VLOG(0) << "Writing out Thread-" << buffer->get_thread_id() << "'s log. write_size="
            << write_size << ", padded_write_size=" << padded_write_size;
        if (write_size == padded_write_size) {
            VLOG(0) << "no padding needed, lucky";
            CHECK_ERROR_CODE(current_file_->write(write_size,
                    memory::AlignedMemorySlice(buffer->buffer_memory_, from_offset, write_size)));
            buffer->offset_durable_ += write_size;
        } else {
            VLOG(1) << "padding needed";
            const uint64_t main_write_size = padded_write_size - SECTOR_SIZE;
            const uint64_t fragment_size = write_size - main_write_size;
            ASSERT_ND(fragment_size < SECTOR_SIZE);
            ASSERT_ND(fragment_size > 0);
            CHECK_ERROR_CODE(current_file_->write(main_write_size,
                memory::AlignedMemorySlice(buffer->buffer_memory_, from_offset, main_write_size)));
            buffer->offset_durable_ += main_write_size;

            // put the remaining and filler log in our fill_buffer_.
            char* buf = reinterpret_cast<char*>(fill_buffer_.get_block());
            std::memcpy(buf, buffer->buffer_ + buffer->offset_durable_, fragment_size);
            FillerLogType* filler_log = reinterpret_cast<FillerLogType*>(buf + fragment_size);
            filler_log->header_.log_type_code_ = get_log_code<FillerLogType>();
            filler_log->header_.log_length_ = SECTOR_SIZE - fragment_size;
            filler_log->header_.storage_id_ = 0;
            CHECK_ERROR_CODE(current_file_->write(SECTOR_SIZE, fill_buffer_));
            buffer->offset_durable_ += fragment_size;
        }
        return RET_OK;
    }
}

}  // namespace log
}  // namespace foedus
