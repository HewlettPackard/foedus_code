/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"

#include <cstring>
#include <ostream>

#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace masstree {
ErrorStack MasstreePartitioner::design_partition() {
  MasstreePartitionerInDesignData context(engine_, id_);
  // VolatilePagePointer root_ptr = storage.get_control_block()->root_page_pointer_.volatile_pointer_;
  return kRetOk;
}

MasstreePage* MasstreePartitionerInDesignData::follow_pointer(DualPagePointer* ptr) {
  Page* page;
  if (ptr->volatile_pointer_.is_null()) {
    ASSERT_ND(ptr->snapshot_pointer_ != 0);
    // engine_->get_memory_manager().get_local_memory().
  } else {
    page = volatile_resolver_.resolve_offset(ptr->volatile_pointer_);
  }
  return reinterpret_cast<MasstreePage*>(page);
}

MasstreePartitionerInDesignData::MasstreePartitionerInDesignData(Engine* engine, StorageId id)
  : engine_(engine),
    id_(id),
    storage_(engine, id),
    volatile_resolver_(engine_->get_memory_manager()->get_global_volatile_page_resolver()) {
  ASSERT_ND(storage_.exists());
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
