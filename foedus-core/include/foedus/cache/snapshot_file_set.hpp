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
#ifndef FOEDUS_CACHE_SNAPSHOT_FILE_SET_HPP_
#define FOEDUS_CACHE_SNAPSHOT_FILE_SET_HPP_

#include <iosfwd>
#include <map>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace cache {
/**
 * @brief Holds a set of read-only file objects for snapshot files.
 * @ingroup CACHE
 * @details
 * In essense, this is a map<snapshot_id, map<node_id, DirectIoFile*> >.
 * Each \b thread memory internally maintains this file set.
 * This is because DirectIoFile (or underlying linux read()) cannot be concurrently used.
 * Each thread thus obtains its own file descriptors using this object.
 * As it's thread-local, no synchronization is needed in this object.
 *
 * This design might hit the maximum number of file descriptors per process.
 * Check cat /proc/sys/fs/file-max if that happens. Google how to change it (soft AND hard limits).
 *
 * @todo So far we really use std::map. But, this is not ideal in terms of performance.
 * node-id is up to 256, snapshots are almost always very few, so we can do array-based
 * something.
 */
class SnapshotFileSet CXX11_FINAL : public DefaultInitializable {
 public:
  explicit SnapshotFileSet(Engine* engine);
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;
  void        close_all();

  Engine*     get_engine() { return engine_; }

  SnapshotFileSet() CXX11_FUNC_DELETE;
  SnapshotFileSet(const SnapshotFileSet &other) CXX11_FUNC_DELETE;
  SnapshotFileSet& operator=(const SnapshotFileSet &other) CXX11_FUNC_DELETE;

  ErrorCode get_or_open_file(storage::SnapshotPagePointer page_pointer, fs::DirectIoFile** out) {
    snapshot::SnapshotId snapshot_id
      = storage::extract_snapshot_id_from_snapshot_pointer(page_pointer);
    thread::ThreadGroupId node_id = storage::extract_numa_node_from_snapshot_pointer(page_pointer);
    return get_or_open_file(snapshot_id, node_id, out);
  }
  ErrorCode get_or_open_file(
    snapshot::SnapshotId snapshot_id,
    thread::ThreadGroupId node_id,
    fs::DirectIoFile** out);

  ErrorCode read_page(storage::SnapshotPagePointer page_id, void* out);

  friend std::ostream&    operator<<(std::ostream& o, const SnapshotFileSet& v);

 private:
  Engine* const engine_;
  std::map<snapshot::SnapshotId, std::map< thread::ThreadGroupId, fs::DirectIoFile* > > files_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_SNAPSHOT_FILE_SET_HPP_
