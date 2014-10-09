/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/composer.hpp"

#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_composer_impl.hpp"
#include "foedus/storage/sequential/sequential_composer_impl.hpp"

namespace foedus {
namespace storage {
std::ostream& operator<<(std::ostream& o, const Composer& v) {
  o << "<Composer>"
    << "<storage_id_>" << v.get_storage_id() << "</storage_id_>"
    << "<storage_type>" << to_storage_type_name(v.get_storage_type()) << "</storage_type>"
    << "</Composer>";
  return o;
}

Composer::Composer(Engine *engine, StorageId storage_id)
  : engine_(engine),
    storage_id_(storage_id),
    storage_type_(engine_->get_storage_manager()->get_storage(storage_id_)->meta_.type_) {}

ErrorStack Composer::compose(const ComposeArguments& args) {
  switch (storage_type_) {
    case kArrayStorage:
      return array::ArrayComposer(this).compose(args);
    case kSequentialStorage:
      return sequential::SequentialComposer(this).compose(args);
    // TODO(Hideaki) implement
    case kMasstreeStorage:
    case kHashStorage:
    default:
      return kRetOk;
  }
}

ErrorStack Composer::construct_root(const ConstructRootArguments& args) {
  switch (storage_type_) {
    case kArrayStorage:
      return array::ArrayComposer(this).construct_root(args);
    case kSequentialStorage:
      return sequential::SequentialComposer(this).construct_root(args);
    // TODO(Hideaki) implement
    case kMasstreeStorage:
    case kHashStorage:
    default:
      return kRetOk;
  }
}

uint64_t Composer::get_required_work_memory_size(
  snapshot::SortedBuffer** log_streams,
  uint32_t log_streams_count) {
  switch (storage_type_) {
    case kArrayStorage:
      return array::ArrayComposer(this).
         get_required_work_memory_size(log_streams, log_streams_count);
    case kSequentialStorage:
      return sequential::SequentialComposer(this).
        get_required_work_memory_size(log_streams, log_streams_count);
    // TODO(Hideaki) implement
    case kMasstreeStorage:
    case kHashStorage:
    default:
      return 0;
  }
}

}  // namespace storage
}  // namespace foedus
