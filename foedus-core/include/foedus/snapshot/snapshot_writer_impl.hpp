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
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief
 * @ingroup SNAPSHOT
 * @details
 */
class SnapshotWriter final : public DefaultInitializable {
 public:
  explicit SnapshotWriter(Engine* engine, LogReducer* parent)
    : engine_(engine), parent_(parent) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  SnapshotWriter() = delete;
  SnapshotWriter(const SnapshotWriter &other) = delete;
  SnapshotWriter& operator=(const SnapshotWriter &other) = delete;

  std::string             to_string() const;
  friend std::ostream&    operator<<(std::ostream& o, const SnapshotWriter& v);

 private:
  Engine* const                   engine_;
  LogReducer* const               parent_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_WRITER_IMPL_HPP_
