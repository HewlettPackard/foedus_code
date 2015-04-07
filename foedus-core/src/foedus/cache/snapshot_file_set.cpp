/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
#include "foedus/storage/page.hpp"

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
    fs::DirectIoFile* file = new fs::DirectIoFile(path);
    ErrorCode open_error = file->open(true, false, false, false);
    if (open_error != kErrorCodeOk) {
      delete file;
      return open_error;
    }
    std::pair< thread::ThreadGroupId, fs::DirectIoFile* > entry(node_id, file);
    the_map.insert(entry);
    *out = file;
    return kErrorCodeOk;
  }
}

ErrorCode SnapshotFileSet::read_page(storage::SnapshotPagePointer page_id, void* out) {
  fs::DirectIoFile* file;
  CHECK_ERROR_CODE(get_or_open_file(page_id, &file));
  storage::SnapshotLocalPageId local_page_id
    = storage::extract_local_page_id_from_snapshot_pointer(page_id);
  CHECK_ERROR_CODE(
    file->seek(local_page_id * sizeof(storage::Page), fs::DirectIoFile::kDirectIoSeekSet));
  CHECK_ERROR_CODE(file->read_raw(sizeof(storage::Page), out));
  ASSERT_ND(reinterpret_cast<storage::Page*>(out)->get_header().page_id_ == page_id);
  return kErrorCodeOk;
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
