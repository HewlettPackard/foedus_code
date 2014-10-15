/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/log_reducer_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/rdtsc_watch.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/snapshot/log_gleaner_impl.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/partitioner.hpp"

namespace foedus {
namespace snapshot {

LogReducer::LogReducer(Engine* engine)
: MapReduceBase(engine, engine->get_soc_id()),
  previous_snapshot_files_(engine_),
  sorted_runs_(0) {
  soc::NodeMemoryAnchors* anchors = engine->get_soc_manager()->get_shared_memory_repo()->
    get_node_memory_anchors(numa_node_);
  control_block_ = anchors->log_reducer_memory_;
  buffers_[0] = anchors->log_reducer_buffers_[0];
  buffers_[1] = anchors->log_reducer_buffers_[1];
  root_info_pages_ = anchors->log_reducer_root_info_pages_;
}

ErrorStack LogReducer::initialize_once() {
  control_block_->initialize();
  control_block_->id_ = engine_->get_soc_id();

  const SnapshotOptions& option = engine_->get_options().snapshot_;

  uint64_t buffer_size = static_cast<uint64_t>(option.log_reducer_buffer_mb_) << 20;
  uint64_t dump_buffer_size = static_cast<uint64_t>(option.log_reducer_dump_io_buffer_mb_) << 20;
  dump_io_buffer_.alloc(
    dump_buffer_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    get_numa_node());
  ASSERT_ND(!dump_io_buffer_.is_null());

  // start from 1/16 of the main buffer. Should be big enough.
  sort_buffer_.alloc(
    buffer_size >> 4,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    get_numa_node());

  // start from 1/16 of the main buffer. Should be big enough.
  positions_buffers_.alloc(
    buffer_size >> 4,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    get_numa_node());
  input_positions_slice_ = memory::AlignedMemorySlice(
    &positions_buffers_,
    0,
    positions_buffers_.get_size() >> 1);
  output_positions_slice_ = memory::AlignedMemorySlice(
    &positions_buffers_,
    positions_buffers_.get_size() >> 1,
    positions_buffers_.get_size() >> 1);

  writer_pool_memory_.alloc(
    static_cast<uint64_t>(option.snapshot_writer_page_pool_size_mb_) << 20,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);

  writer_intermediate_memory_.alloc(
    static_cast<uint64_t>(option.snapshot_writer_intermediate_pool_size_mb_) << 20,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_),

  sorted_runs_ = 0;

  // we don't initialize snapshot_writer_/composer_work_memory_ yet
  // because they are needed at the end of reducer.
  return kRetOk;
}

ErrorStack LogReducer::uninitialize_once() {
  ErrorStackBatch batch;
  batch.emprace_back(previous_snapshot_files_.uninitialize());
  writer_intermediate_memory_.release_block();
  writer_pool_memory_.release_block();
  dump_io_buffer_.release_block();
  sort_buffer_.release_block();
  positions_buffers_.release_block();
  control_block_->uninitialize();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack LogReducer::handle_process() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    WRAP_ERROR_CODE(check_cancelled());
    if (parent_.is_all_mappers_completed()) {
      break;
    }
    // should I switch the current buffer?
    // this is while, not if, in case the new current buffer becomes full while this reducer is
    // dumping the old current buffer.
    while (control_block_->get_current_buffer_status().is_no_more_writers()) {
      WRAP_ERROR_CODE(check_cancelled());
      // okay, let's switch now. As this thread dumps the buffer as soon as this happens,
      // only one of the buffers can be full.
      if (!control_block_->get_non_current_buffer_status().is_clear()) {
        LOG(FATAL) << to_string() << " wtf. both buffers are in use, can't happen";
      }
      LOG(INFO) << to_string() << " switching buffer. current_buffer_="
        << control_block_->current_buffer_;
      control_block_->current_buffer_.fetch_add(1U);
      ASSERT_ND(sorted_runs_ + 1U == control_block_->current_buffer_);
      // Then, immediately start dumping the full buffer.
      CHECK_ERROR(dump_buffer());
    }
  }

  LOG(INFO) << to_string() << " all mappers are done, this reducer starts the merge-sort phase.";
  ASSERT_ND(parent_.is_all_mappers_completed());
  WRAP_ERROR_CODE(check_cancelled());
  CHECK_ERROR(merge_sort());

  LOG(INFO) << to_string() << " all done.";
  return kRetOk;
}

ErrorStack LogReducer::dump_buffer() {
  LOG(INFO) << "Sorting and dumping " << to_string() << "'s buffer to a file."
    << " current sorted_runs_=" << sorted_runs_;
  uint32_t buffer_index = sorted_runs_ % 2;
  if (!control_block_->get_buffer_status_atomic(buffer_index).is_no_more_writers()) {
    LOG(FATAL) << "wtf. this buffer is still open for writers";
  }

  dump_buffer_wait_for_writers(buffer_index);
  WRAP_ERROR_CODE(check_cancelled());

  ReducerBufferStatus final_status = control_block_->get_buffer_status_atomic(buffer_index);
  debugging::StopWatch stop_watch;
  LOG(INFO) << to_string() << " Started sort/dump " <<
    from_buffer_position(final_status.components.tail_position_) << " bytes of logs";

  char* const base = reinterpret_cast<char*>(buffers_[buffer_index]);
  std::map<storage::StorageId, std::vector<BufferPosition> > blocks;
  dump_buffer_scan_block_headers(base, final_status.components.tail_position_, &blocks);

  // open a file
  fs::Path path = get_sorted_run_file_path(sorted_runs_);
  fs::DirectIoFile file(path);
  WRAP_ERROR_CODE(file.open(false, true, true, true));
  LOG(INFO) << to_string() << " Created a sorted run file " << path;

  // for each storage (ordered by storage ID), sort and dump them into the file.
  for (auto& kv : blocks) {
    WRAP_ERROR_CODE(check_cancelled());
    LogBuffer log_buffer(base);
    storage::StorageId storage_id = kv.first;
    uint32_t written_count;
    CHECK_ERROR(dump_buffer_sort_storage(log_buffer, storage_id, kv.second, &written_count));
    // write them out to the file
    BufferPosition* outputs
      = reinterpret_cast<BufferPosition*>(output_positions_slice_.get_block());
    CHECK_ERROR(dump_buffer_sort_storage_write(
      log_buffer,
      storage_id,
      outputs,
      written_count,
      &file));
  }

  // we don't need fsync here. if there is a failure during snapshotting,
  // we just start over. logs are already durable.
  file.close();

  stop_watch.stop();
  LOG(INFO) << to_string() << " Done sort/dump " <<
    from_buffer_position(final_status.components.tail_position_) << " bytes in "
    << stop_watch.elapsed_ms() << "ms"
    << " dumped file length=" << fs::file_size(path);

  // clear the status so that mappers can start using this buffer.
  // note that this reducer has to do the buffer switch before mapper can really start using it.
  // this reducer immediately checks if it should do so right after this function.
  ++sorted_runs_;
  control_block_->buffer_status_[buffer_index] = 0;
  return kRetOk;
}

ErrorStack LogReducer::dump_buffer_wait_for_writers(uint32_t buffer_index) const {
  debugging::StopWatch wait_watch;
  SPINLOCK_WHILE(control_block_->get_buffer_status_atomic(buffer_index).get_active_writers() > 0) {
    WRAP_ERROR_CODE(check_cancelled());
  }
  wait_watch.stop();
  LOG(INFO) << to_string() << " Okay, now active_writers==0. waited/looped for "
    << wait_watch.elapsed_us() << "us";
  // I'd be very surprised if we were waiting for more than 1000us.
  return kRetOk;
}

void LogReducer::dump_buffer_scan_block_headers(
  char* buffer_base,
  BufferPosition tail_position,
  std::map<storage::StorageId, std::vector<BufferPosition> > *blocks) const {
  debugging::StopWatch header_watch;
  const uint64_t end = from_buffer_position(tail_position);
  uint64_t cur = 0;
  uint32_t total_blocks = 0;
  while (cur < end) {
    FullBlockHeader* header = reinterpret_cast<FullBlockHeader*>(buffer_base + cur);
    if (!header->is_full_block()) {
      LOG(FATAL) << to_string() << " wtf. magic word doesn't match. cur=" << cur;
    }
    auto it = blocks->find(header->storage_id_);
    if (it != blocks->end()) {
      it->second.push_back(to_buffer_position(cur));
    } else {
      std::vector<BufferPosition> vec;
      vec.reserve(1 << 10);
      vec.push_back(to_buffer_position(cur));
      blocks->insert(std::pair< storage::StorageId, std::vector<BufferPosition> >(
        header->storage_id_, vec));
    }
    cur += from_buffer_position(header->block_length_);
    ++total_blocks;
  }
  ASSERT_ND(cur == end);
  header_watch.stop();
  LOG(INFO) << to_string() << " Scanned all blocks. There were " << total_blocks << " blocks"
    << ", " << blocks->size() << " distinct storages."
    << " scan elapsed time=" << header_watch.elapsed_us() << "us";
}

ErrorStack LogReducer::dump_buffer_sort_storage(
  const LogBuffer &buffer,
  storage::StorageId storage_id,
  const std::vector<BufferPosition>& log_positions,
  uint32_t* written_count) {
  // first, count how many log entries are there. this is quick as we have a statistics
  // in the header.
  uint64_t records = 0;
  for (BufferPosition position : log_positions) {
    FullBlockHeader* header = reinterpret_cast<FullBlockHeader*>(buffer.resolve(position));
    if (!header->is_full_block()) {
      LOG(FATAL) << to_string() << " wtf. magic word doesn't match. position=" << position
        << ", storage_id=" << storage_id;
    }
    records += header->log_count_;
  }

  // now we need a memory for this long array. expand the memory if not sufficient.
  uint64_t positions_buffer_size = records * sizeof(BufferPosition);
  expand_positions_buffers_if_needed(positions_buffer_size);
  BufferPosition* inputs = reinterpret_cast<BufferPosition*>(input_positions_slice_.get_block());
  uint64_t cur_rec_total = 0;

  // put all log positions to the array
  for (BufferPosition position : log_positions) {
    FullBlockHeader* header = reinterpret_cast<FullBlockHeader*>(buffer.resolve(position));
    if (!header->is_full_block()) {
      LOG(FATAL) << to_string() << " wtf. magic word doesn't match. position=" << position
        << ", storage_id=" << storage_id;
    }
    BufferPosition record_pos = position + to_buffer_position(sizeof(FullBlockHeader));
    for (uint32_t i = 0; i < header->log_count_; ++i) {
      log::RecordLogType* record = buffer.resolve(record_pos);
      ASSERT_ND(record->header_.storage_id_ == storage_id);
      ASSERT_ND(record->header_.log_length_ > 0);
      inputs[cur_rec_total] = record_pos;
      ++cur_rec_total;
      record_pos += to_buffer_position(record->header_.log_length_);
    }
    ASSERT_ND(record_pos == position + header->block_length_);
  }
  ASSERT_ND(cur_rec_total == records);

  // Now, sort these log records by key and then ordinal. we use the partitioner object for this.
  storage::Partitioner partitioner(engine_, storage_id);
  BufferPosition* pos = reinterpret_cast<BufferPosition*>(output_positions_slice_.get_block());
  *written_count = 0;
  storage::Partitioner::SortBatchArguments args = {
    buffer,
    inputs,
    static_cast<uint32_t>(records),
    &sort_buffer_,
    parent_.get_base_epoch(),
    pos,
    written_count};
  partitioner.sort_batch(args);
  return kRetOk;
}

uint64_t LogReducer::dump_block_header(
  const LogBuffer &buffer,
  storage::StorageId storage_id,
  const BufferPosition* sorted_logs,
  uint32_t log_count,
  void* destination) const {
  // figuring out the block length is a bit expensive. we have to go through all log entries.
  // but, snapshotting happens only once per minutes, and all of these are in-memory operations.
  // I hope this isn't a big cost. (let's keep an eye on it, though)
  debugging::StopWatch length_watch;
  uint64_t total_bytes = sizeof(FullBlockHeader);
  for (uint32_t i = 0; i < log_count; ++i) {
    total_bytes += buffer.resolve(sorted_logs[i])->header_.log_length_;
  }
  length_watch.stop();
  LOG(INFO) << to_string() << " iterated over " << log_count
    << " log records to figure out block length in "<< length_watch.elapsed_us() << "us";

  FullBlockHeader* header = reinterpret_cast<FullBlockHeader*>(destination);
  header->storage_id_ = storage_id;
  header->log_count_ = log_count;
  header->magic_word_ = BlockHeaderBase::kFullBlockHeaderMagicWord;
  header->block_length_ = to_buffer_position(total_bytes);
  return total_bytes;
}

ErrorStack LogReducer::dump_buffer_sort_storage_write(
  const LogBuffer &buffer,
  storage::StorageId storage_id,
  const BufferPosition* sorted_logs,
  uint32_t log_count,
  fs::DirectIoFile *dump_file) {
  debugging::StopWatch write_watch;
  char* io_buffer = reinterpret_cast<char*>(dump_io_buffer_.get_block());
  // we flush the IO buffer when we wrote out this number of bytes.
  // to keep it aligned, the bytes after this threshold have to be retained and copied over to
  // the beginning of the buffer.
  const uint64_t flush_threshold = dump_io_buffer_.get_size() - (1 << 16);
  uint64_t total_bytes = dump_block_header(buffer, storage_id, sorted_logs, log_count, io_buffer);
  uint64_t total_written = 0;
  uint64_t current_pos = sizeof(FullBlockHeader);
  for (uint32_t i = 0; i < log_count; ++i) {
    const log::RecordLogType* record = buffer.resolve(sorted_logs[i]);
    ASSERT_ND(current_pos % 8 == 0);
    ASSERT_ND(record->header_.storage_id_ == storage_id);
    ASSERT_ND(record->header_.log_length_ > 0);
    ASSERT_ND(record->header_.log_length_ % 8 == 0);
    std::memcpy(io_buffer + current_pos, record, record->header_.log_length_);
    current_pos += record->header_.log_length_;
    if (current_pos >= flush_threshold) {
      WRAP_ERROR_CODE(dump_file->write(flush_threshold, dump_io_buffer_));

      // move the fragment to beginning
      if (current_pos > flush_threshold) {
        std::memcpy(io_buffer, io_buffer + flush_threshold, current_pos - flush_threshold);
      }
      current_pos -= flush_threshold;
      total_written += flush_threshold;
    }
  }

  ASSERT_ND(total_bytes == current_pos);  // now we went over all logs again

  if (current_pos > 0) {
    ASSERT_ND(current_pos < flush_threshold);
    // for aligned write, add a dummy storage block at the end.
    if (current_pos % (log::FillerLogType::kLogWriteUnitSize) != 0) {
      uint64_t upto = assorted::align<uint64_t, log::FillerLogType::kLogWriteUnitSize>(current_pos);
      ASSERT_ND(upto > current_pos);
      ASSERT_ND(upto < current_pos + log::FillerLogType::kLogWriteUnitSize);
      ASSERT_ND(upto % log::FillerLogType::kLogWriteUnitSize == 0);
      FillerBlockHeader* filler = reinterpret_cast<FillerBlockHeader*>(io_buffer + current_pos);
      filler->block_length_ = to_buffer_position(upto - current_pos);
      ASSERT_ND(filler->block_length_ < to_buffer_position(log::FillerLogType::kLogWriteUnitSize));
      filler->magic_word_ = BlockHeaderBase::kFillerBlockHeaderMagicWord;
      if (upto - current_pos > sizeof(FillerBlockHeader)) {
        // fill it with zeros. not mandatory, but wouldn't hurt. it's just 4kb.
        std::memset(
          io_buffer + current_pos + sizeof(FillerBlockHeader),
          0,
          upto - current_pos - sizeof(FillerBlockHeader));
      }
      current_pos = upto;
    }

    ASSERT_ND(current_pos % log::FillerLogType::kLogWriteUnitSize == 0);
    WRAP_ERROR_CODE(dump_file->write(current_pos, dump_io_buffer_));
    total_written += current_pos;
  }

  ASSERT_ND(total_written % log::FillerLogType::kLogWriteUnitSize == 0);
  write_watch.stop();
  LOG(INFO) << to_string() << " Wrote out storage-" << storage_id << " which had " << log_count
    << " log records (" << total_written << " bytes) in "<< write_watch.elapsed_ms() << "ms";
  return kRetOk;
}


void LogReducer::expand_if_needed(
  uint64_t required_size,
  memory::AlignedMemory *memory,
  const std::string& name) {
  if (memory->is_null() || memory->get_size() < required_size) {
    if (memory->is_null()) {
      LOG(INFO) << to_string() << " initially allocating " << name << "."
        << assorted::Hex(required_size) << " bytes.";
    } else {
      LOG(WARNING) << to_string() << " automatically expanding " << name << " from "
        << assorted::Hex(memory->get_size()) << " bytes to "
        << assorted::Hex(required_size) << " bytes. if this happens often,"
        << " our sizing is wrong.";
    }
    memory->alloc(
      required_size,
      memory::kHugepageSize,
      memory::AlignedMemory::kNumaAllocOnnode,
      get_numa_node());
  }
}
void LogReducer::expand_positions_buffers_if_needed(uint64_t required_size_per_buffer) {
  ASSERT_ND(input_positions_slice_.get_size() == output_positions_slice_.get_size());
  if (input_positions_slice_.get_size() < required_size_per_buffer) {
    uint64_t new_size = required_size_per_buffer * 2;
    LOG(WARNING) << to_string() << " automatically expanding positions_buffers from "
      << positions_buffers_.get_size() << " to " << new_size << ". if this happens often,"
      << " our sizing is wrong.";
      positions_buffers_.alloc(
        new_size,
        memory::kHugepageSize,
        memory::AlignedMemory::kNumaAllocOnnode,
        get_numa_node());
      input_positions_slice_ = memory::AlignedMemorySlice(
        &positions_buffers_,
        0,
        positions_buffers_.get_size() >> 1);
      output_positions_slice_ = memory::AlignedMemorySlice(
        &positions_buffers_,
        positions_buffers_.get_size() >> 1,
        positions_buffers_.get_size() >> 1);
  }
}

fs::Path LogReducer::get_sorted_run_file_path(uint32_t sorted_run) const {
  // sorted_run_<snapshot id>_<node id>_<sorted run>.tmp is the file name
  std::stringstream file_name;
  file_name << "/sorted_run_"
    << parent_.get_snapshot_id() << "_"
    << static_cast<int>(get_numa_node()) << "_"
    << static_cast<int>(sorted_run) << ".tmp";
  fs::Path path(engine_->get_options().snapshot_.convert_folder_path_pattern(get_numa_node()));
  path /= file_name.str();
  return path;
}

LogReducer::MergeContext::MergeContext(uint32_t dumped_files_count)
  : dumped_files_count_(dumped_files_count),
  tmp_sorted_buffer_array_(new SortedBuffer*[dumped_files_count + 1]),
  tmp_sorted_buffer_count_(0) {
}

LogReducer::MergeContext::~MergeContext() {
  sorted_buffers_.clear();
  // destructor calls close(), but to make sure
  for (auto& file : sorted_files_auto_ptrs_) {
    file->close();
  }
  sorted_files_auto_ptrs_.clear();
  io_buffers_.clear();
  io_memory_.release_block();
  delete[] tmp_sorted_buffer_array_;
  tmp_sorted_buffer_array_ = nullptr;
}

storage::StorageId LogReducer::MergeContext::get_min_storage_id() const {
  bool first = true;
  storage::StorageId storage_id = 0;
  for (uint32_t i = 0 ; i < sorted_buffers_.size(); ++i) {
    storage::StorageId the_storage_id = sorted_buffers_[i]->get_cur_block_storage_id();
    if (the_storage_id == 0) {
      continue;
    }
    if (first) {
      storage_id = the_storage_id;
      first = false;
    } else {
      storage_id = std::min(storage_id, the_storage_id);
    }
  }
  return storage_id;
}

void LogReducer::MergeContext::set_tmp_sorted_buffer_array(storage::StorageId storage_id) {
  tmp_sorted_buffer_count_ = 0;
  for (uint32_t i = 0 ; i < sorted_buffers_.size(); ++i) {
    if (sorted_buffers_[i]->get_cur_block_storage_id() == storage_id) {
        tmp_sorted_buffer_array_[tmp_sorted_buffer_count_] = sorted_buffers_[i].get();
        ++tmp_sorted_buffer_count_;
    }
  }
  ASSERT_ND(tmp_sorted_buffer_count_ > 0);
}

ErrorStack LogReducer::merge_sort() {
  merge_sort_check_buffer_status();
  CHECK_ERROR(merge_sort_dump_last_buffer());

  // The writer to writes out composed snapshot pages to a new snapshot file.
  // we use it only during merge_sort().
  SnapshotWriter snapshot_writer(
    engine_,
    numa_node_,
    parent_.get_snapshot_id(),
    &writer_pool_memory_,
    &writer_intermediate_memory_);
  CHECK_ERROR(snapshot_writer.open());

  CHECK_ERROR(previous_snapshot_files_.initialize());

  // because now we are at the last merging phase, we will no longer dump sorted runs any more.
  // thus, we release the reducer's dump IO buffer to reduce memory pressure.
  dump_io_buffer_.release_block();

  MergeContext context(sorted_runs_);
  LOG(INFO) << to_string() << " merge sorting " << sorted_runs_ << " sorted runs and the current"
    << " buffer which has " << control_block_->get_current_buffer_status().get_tail_position()
    << " bytes";
  debugging::StopWatch merge_watch;

  // prepare the input streams for composers
  merge_sort_allocate_io_buffers(&context);
  CHECK_ERROR(merge_sort_open_sorted_runs(&context));
  CHECK_ERROR(merge_sort_initialize_sort_buffers(&context));

  // this work memory automatically expands if needed
  memory::AlignedMemory composer_work_memory;
  composer_work_memory.alloc(
    1U << 21,
    1U << 12,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);

  // merge-sort each storage
  storage::StorageId prev_storage_id = 0;
  control_block_->total_storage_count_ = 0;
  for (storage::StorageId storage_id = context.get_min_storage_id();
        storage_id > 0;
        storage_id = context.get_min_storage_id(), ++control_block_->total_storage_count_) {
    if (storage_id <= prev_storage_id) {
      LOG(FATAL) << to_string() << " wtf. not storage sorted? " << *this;
    }
    prev_storage_id = storage_id;

    // collect streams for this storage
    VLOG(0) << to_string() << " merging storage-" << storage_id << ", num="
      << control_block_->total_storage_count_;
    context.set_tmp_sorted_buffer_array(storage_id);

    // run composer
    storage::Composer composer(engine_, storage_id);
    // snapshot_reader_.get_or_open_file();
    ASSERT_ND(control_block_->total_storage_count_ <= get_max_storage_count());
    storage::Page* root_info_page = root_info_pages_ + control_block_->total_storage_count_;
    storage::Composer::ComposeArguments args = {
      &snapshot_writer,
      &previous_snapshot_files_,
      context.tmp_sorted_buffer_array_,
      context.tmp_sorted_buffer_count_,
      &composer_work_memory,
      root_info_page};
    CHECK_ERROR(composer.compose(args));

    // move on to next blocks
    for (uint32_t i = 0 ; i < context.sorted_buffers_.size(); ++i) {
      SortedBuffer *buffer = context.sorted_buffers_[i].get();
      WRAP_ERROR_CODE(merge_sort_advance_sort_buffers(buffer, storage_id));
    }
  }
  ASSERT_ND(control_block_->total_storage_count_ <= get_max_storage_count());

  snapshot_writer.close();
  merge_watch.stop();
  LOG(INFO) << to_string() << " completed merging in " << merge_watch.elapsed_sec() << " seconds"
    << " . total_storage_count_=" << control_block_->total_storage_count_;
  return kRetOk;
}


void LogReducer::merge_sort_check_buffer_status() const {
  ASSERT_ND(sorted_runs_ == control_block_->current_buffer_);
  if (control_block_->get_non_current_buffer_status().components.tail_position_ > 0 ||
      control_block_->get_non_current_buffer_status().components.active_writers_ > 0) {
    LOG(FATAL) << to_string() << " non-current buffer still has some data. this must not happen"
      << " at merge_sort step.";
  }
  ReducerBufferStatus cur_status = control_block_->get_current_buffer_status();
  if (cur_status.components.active_writers_ > 0) {
    LOG(FATAL) << to_string() << " last buffer is still being written. this must not happen"
      << " at merge_sort step.";
  }
  ASSERT_ND(!cur_status.is_no_more_writers());  // it should be still active
}

void LogReducer::merge_sort_allocate_io_buffers(LogReducer::MergeContext* context) const {
  if (context->sorted_buffers_.size() == 0) {
    LOG(INFO) << to_string() << " great, no sorted run files. everything in-memory";
    return;
  }
  debugging::StopWatch alloc_watch;
  uint64_t size_per_run =
    static_cast<uint64_t>(engine_->get_options().snapshot_.log_reducer_read_io_buffer_kb_) << 10;
  uint64_t size_total = size_per_run * context->sorted_buffers_.size();
  context->io_memory_.alloc(
    size_total,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    get_numa_node());
  for (uint32_t i = 0; i < context->sorted_buffers_.size(); ++i) {
    context->io_buffers_.emplace_back(memory::AlignedMemorySlice(
      &context->io_memory_,
      i * size_per_run,
      size_per_run));
  }
  alloc_watch.stop();
  LOG(INFO) << to_string() << " allocated IO buffers (" << size_total << " bytes in total) "
    << " in " << alloc_watch.elapsed_us() << "us";
}
ErrorStack LogReducer::merge_sort_dump_last_buffer() {
  uint16_t last = control_block_->current_buffer_ % 2;
  BufferPosition last_pos = control_block_->get_buffer_status_atomic(last).get_tail_position();
  LOG(INFO) << to_string() << " sorting the last buffer in memory (" << last_pos * 8 << "B)...";
  debugging::StopWatch watch;

  // Reuse most of dump_buffer_xxx methods
  char* const base = reinterpret_cast<char*>(buffers_[last]);
  char* const other = reinterpret_cast<char*>(buffers_[last + 1]);
  std::map<storage::StorageId, std::vector<BufferPosition> > blocks;
  dump_buffer_scan_block_headers(base, last_pos, &blocks);
  uint64_t other_bytes = 0;
  for (auto& kv : blocks) {
    LogBuffer buffer(base);
    storage::StorageId storage_id = kv.first;
    uint32_t count;
    CHECK_ERROR(dump_buffer_sort_storage(buffer, storage_id, kv.second, &count));
    BufferPosition* pos = reinterpret_cast<BufferPosition*>(output_positions_slice_.get_block());

      // The only difference is here. Output the sorted result to the other buffer, not to file.
    uint64_t total_bytes = dump_block_header(buffer, storage_id, pos, count, other);
    uint64_t current_pos = sizeof(FullBlockHeader);
    for (uint32_t i = 0; i < count; ++i) {
      const log::RecordLogType* record = buffer.resolve(pos[i]);
      ASSERT_ND(current_pos % 8 == 0);
      ASSERT_ND(record->header_.storage_id_ == storage_id);
      ASSERT_ND(record->header_.log_length_ > 0);
      ASSERT_ND(record->header_.log_length_ % 8 == 0);
      std::memcpy(other + current_pos, record, record->header_.log_length_);
      current_pos += record->header_.log_length_;
    }
    ASSERT_ND(total_bytes == current_pos);  // now we went over all logs again
    other_bytes += total_bytes;
  }

  // We wrote out to the other buffer, switch the current.
  ++control_block_->current_buffer_;
  ReducerBufferStatus other_status;
  other_status.components.active_writers_ = 0;
  other_status.components.flags_ = kFlagNoMoreWriters;
  other_status.components.tail_position_ = to_buffer_position(other_bytes);
  control_block_->buffer_status_[control_block_->current_buffer_ % 2] = other_status.word;
  watch.stop();
  LOG(INFO) << to_string() << " sorted the last buffer in memory (" << last_pos * 8 << "B -> "
    << other_bytes << "B)  in " << watch.elapsed_ms() << "ms";
  return kRetOk;
}

ErrorStack LogReducer::merge_sort_open_sorted_runs(LogReducer::MergeContext* context) const {
  uint32_t last_buffer_index = control_block_->current_buffer_;
  ReducerBufferStatus buffer_status = control_block_->get_buffer_status_atomic(last_buffer_index);
  void* last_buffer = buffers_[last_buffer_index % 2];
  // always the last buffer (no cost)
  context->sorted_buffers_.emplace_back(new InMemorySortedBuffer(
    reinterpret_cast<char*>(last_buffer),
    from_buffer_position(buffer_status.components.tail_position_)));

  // sorted run files
  ASSERT_ND(context->io_buffers_.size() == sorted_runs_);
  for (uint32_t sorted_run = 0 ; sorted_run < context->dumped_files_count_; ++sorted_run) {
    fs::Path path = get_sorted_run_file_path(sorted_run);
    if (!fs::exists(path)) {
      LOG(FATAL) << to_string() << " wtf. this sorted run file doesn't exist " << path;
    }
    uint64_t file_size = fs::file_size(path);
    if (file_size == 0) {
      LOG(FATAL) << to_string() << " wtf. this sorted run file is empty " << path;
    }

    std::unique_ptr<fs::DirectIoFile> file_ptr(new fs::DirectIoFile(
      path,
      engine_->get_options().snapshot_.emulation_));
    WRAP_ERROR_CODE(file_ptr->open(true, false, false, false));

    context->sorted_buffers_.emplace_back(new DumpFileSortedBuffer(
      file_ptr.get(),
      context->io_buffers_[sorted_run]));
    context->sorted_files_auto_ptrs_.emplace_back(std::move(file_ptr));
  }

  ASSERT_ND(context->sorted_files_auto_ptrs_.size() == context->sorted_buffers_.size() - 1U);
  ASSERT_ND(context->dumped_files_count_ == context->sorted_buffers_.size() - 1U);
  return kRetOk;
}

ErrorStack LogReducer::merge_sort_initialize_sort_buffers(LogReducer::MergeContext* context) const {
  for (uint32_t index = 0 ; index < context->sorted_buffers_.size(); ++index) {
    SortedBuffer* buffer = context->sorted_buffers_[index].get();
    if (buffer->get_total_size() == 0) {
      buffer->invalidate_current_block();
      LOG(INFO) << to_string() << " buffer-" << index << " is empty";
      continue;
    }
    if (index > 0) {
      DumpFileSortedBuffer* casted = dynamic_cast<DumpFileSortedBuffer*>(buffer);
      ASSERT_ND(casted);
      // the buffer hasn't loaded any data, so let's make the first read.
      uint64_t desired_reads = std::min(casted->get_buffer_size(), casted->get_total_size());
      WRAP_ERROR_CODE(casted->get_file()->read(desired_reads, casted->get_io_buffer()));
    } else {
      ASSERT_ND(dynamic_cast<InMemorySortedBuffer*>(buffer));
      // in-memory one has already loaded everything
    }

    // See the first block header. As dummy block always follows a real block, this must be
    // a real block.
    const FullBlockHeader* header = reinterpret_cast<const FullBlockHeader*>(
      buffer->get_buffer());
    if (!header->is_full_block()) {
      LOG(FATAL) << to_string() << " wtf. first block in the file is not a real storage block."
        << *buffer;
    }
    buffer->set_current_block(
      header->storage_id_,
      header->log_count_,
      sizeof(FullBlockHeader),
      from_buffer_position(header->block_length_));
  }

  return kRetOk;
}


ErrorCode LogReducer::merge_sort_advance_sort_buffers(
  SortedBuffer* buffer,
  storage::StorageId processed_storage_id) const {
  if (buffer->get_cur_block_storage_id() != processed_storage_id) {
    return kErrorCodeOk;
  }
  uint64_t next_block_header_pos = buffer->get_cur_block_abosulte_end();
  uint64_t in_buffer_pos = buffer->to_relative_pos(next_block_header_pos);
  const BlockHeaderBase* next_header
    = reinterpret_cast<const BlockHeaderBase*>(buffer->get_buffer() + in_buffer_pos);

  // skip a dummy block
  if (next_block_header_pos < buffer->get_total_size() && next_header->is_filler()) {
    // next block is a dummy block. we have to skip over it.
    // no two filler blocks come in a row, so just skip this one.
    uint64_t skip_bytes = from_buffer_position(next_header->block_length_);
    next_block_header_pos += skip_bytes;
    VLOG(1) << to_string() << " skipped a filler block. " << skip_bytes << " bytes";
    if (next_block_header_pos + sizeof(FullBlockHeader)
      > buffer->get_offset() + buffer->get_buffer_size()) {
      // we have to at least read the header of next block. if we unluckily hit
      // the boundary here, wind it.
      LOG(INFO) << to_string() << " wow, we unluckily hit buffer boundary while skipping"
        << " a filler block. it's rare!";
      CHECK_ERROR_CODE(buffer->wind(next_block_header_pos));
      ASSERT_ND(next_block_header_pos >= buffer->get_offset());
      ASSERT_ND(next_block_header_pos + sizeof(FullBlockHeader)
        <= buffer->get_offset() + buffer->get_buffer_size());
    }

    in_buffer_pos = buffer->to_relative_pos(next_block_header_pos);
    next_header =
      reinterpret_cast<const BlockHeaderBase*>(buffer->get_buffer() + in_buffer_pos);
  }

  // next block must be a real block. but, did we reach end of file?
  if (next_block_header_pos >= buffer->get_total_size()) {
    ASSERT_ND(next_block_header_pos == buffer->get_total_size());
    // this stream is done
    buffer->invalidate_current_block();
    LOG(INFO) << to_string() << " fully merged a stream: " << *buffer;
  } else {
    if (!next_header->is_full_block()) {
      LOG(FATAL) << to_string() << " wtf. block magic word doesn't match. pos="
        << next_block_header_pos << ", magic=" << assorted::Hex(next_header->magic_word_);
    }

    const FullBlockHeader* next_header_casted
      = reinterpret_cast<const FullBlockHeader*>(next_header);
    if (next_header_casted->storage_id_ == 0 ||
      next_header_casted->log_count_ == 0 ||
      next_header_casted->block_length_ == 0) {
      LOG(FATAL) << to_string() << " wtf. invalid block header. pos="
        << next_block_header_pos;
    }
    buffer->set_current_block(
      next_header_casted->storage_id_,
      next_header_casted->log_count_,
      next_block_header_pos + sizeof(FullBlockHeader),
      next_block_header_pos + from_buffer_position(next_header_casted->block_length_));
  }
  return kErrorCodeOk;
}

std::ostream& operator<<(std::ostream& o, const LogReducer& v) {
  o << "<LogReducer>"
    << "<id_>" << v.get_id() << "</id_>"
    << "<numa_node>" << v.get_numa_node() << "</numa_node>"
    << "<total_storage_count>" << v.control_block_->total_storage_count_ << "</total_storage_count>"
    << "<sort_buffer_>" << v.sort_buffer_ << "</sort_buffer_>"
    << "<positions_buffers_>" << v.positions_buffers_ << "</positions_buffers_>"
    << "<current_buffer_>" << v.control_block_->current_buffer_ << "</current_buffer_>"
    << "<sorted_runs_>" << v.sorted_runs_ << "</sorted_runs_>"
    << "</LogReducer>";
  return o;
}

uint32_t LogReducer::get_max_storage_count() const {
  return engine_->get_options().storage_.max_storages_;
}


}  // namespace snapshot
}  // namespace foedus
