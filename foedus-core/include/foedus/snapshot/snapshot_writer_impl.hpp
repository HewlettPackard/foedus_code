/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief Writes out one snapshot file for all data pages in one reducer.
 * @ingroup SNAPSHOT
 * @details
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class SnapshotWriter final : public DefaultInitializable {
 public:
  SnapshotWriter(Engine* engine, LogReducer* parent);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  SnapshotWriter() = delete;
  SnapshotWriter(const SnapshotWriter &other) = delete;
  SnapshotWriter& operator=(const SnapshotWriter &other) = delete;

  std::string             to_string() const { return "SnapshotWriter-" + std::to_string(id_); }
  friend std::ostream&    operator<<(std::ostream& o, const SnapshotWriter& v);

 private:
  Engine* const                   engine_;
  LogReducer* const               parent_;
  /** Also parent's ID. One NUMA node = one reducer = one snapshot writer. */
  thread::ThreadGroupId const     id_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
