/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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

  SnapshotFileSet() CXX11_FUNC_DELETE;
  SnapshotFileSet(const SnapshotFileSet &other) CXX11_FUNC_DELETE;
  SnapshotFileSet& operator=(const SnapshotFileSet &other) CXX11_FUNC_DELETE;

  ErrorCode get_or_open_file(storage::SnapshotPagePointer page_pointer, fs::DirectIoFile** out) {
    return get_or_open_file(
      storage::extract_snapshot_id_from_snapshot_pointer(page_pointer),
      storage::extract_numa_node_from_snapshot_pointer(page_pointer),
      out);
  }
  ErrorCode get_or_open_file(
    snapshot::SnapshotId snapshot_id,
    thread::ThreadGroupId node_id,
    fs::DirectIoFile** out);

  friend std::ostream&    operator<<(std::ostream& o, const SnapshotFileSet& v);

 private:
  Engine* const engine_;
  std::map<snapshot::SnapshotId, std::map< thread::ThreadGroupId, fs::DirectIoFile* > > files_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_SNAPSHOT_FILE_SET_HPP_
