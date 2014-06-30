/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/cache/snapshot_file_set.hpp"

#include <map>
#include <ostream>
#include <utility>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/path.hpp"

namespace foedus {
namespace cache {

SnapshotFileSet::SnapshotFileSet(Engine* engine) : engine_(engine) {
}

ErrorStack SnapshotFileSet::initialize_once() {
  return kRetOk;
}

ErrorStack SnapshotFileSet::uninitialize_once() {
  ErrorStackBatch batch;
  close_all();
  return SUMMARIZE_ERROR_BATCH(batch);
}

void SnapshotFileSet::close_all() {
  for (auto& files_in_a_snapshot : files_) {
    auto& values = files_in_a_snapshot.second;
    for (auto& file : values) {
      file.second->close();
      delete file.second;
    }
    values.clear();
  }
  files_.clear();
}

ErrorCode SnapshotFileSet::get_or_open_file(
  snapshot::SnapshotId snapshot_id,
  thread::ThreadGroupId node_id,
  fs::DirectIoFile** out) {
  *out = nullptr;
  auto snapshot = files_.find(snapshot_id);
  if (snapshot == files_.end()) {
    files_.insert(
      std::pair<snapshot::SnapshotId, std::map< thread::ThreadGroupId, fs::DirectIoFile* > >(
        snapshot_id, std::map< thread::ThreadGroupId, fs::DirectIoFile* >()));
    snapshot = files_.find(snapshot_id);
  }
  ASSERT_ND(snapshot != files_.end());
  ASSERT_ND(snapshot->first == snapshot_id);
  auto& the_map = snapshot->second;
  auto node = the_map.find(node_id);
  if (node != the_map.end()) {
    *out = node->second;
    return kErrorCodeOk;
  } else {
    fs::Path path(engine_->get_options().snapshot_.construct_snapshot_file_path(
      snapshot_id,
      node_id));
    std::pair< thread::ThreadGroupId, fs::DirectIoFile* > entry(
      node_id,
      new fs::DirectIoFile( path));
    the_map.insert(entry);
    *out = entry.second;
    return kErrorCodeOk;
  }
}

std::ostream& operator<<(std::ostream& o, const SnapshotFileSet& v) {
  o << "<SnapshotFileSet>";
  for (const auto& snapshot : v.files_) {
    o << "<snapshot id=\"" << snapshot.first << "\">";
    for (const auto& entry : snapshot.second) {
      o << "<node id=\"" << entry.first
        << "\" fd=\"" << entry.second->get_descriptor() << "\" />";
    }
    o << "</snapshot>";
  }
  return o;
}

}  // namespace cache
}  // namespace foedus
