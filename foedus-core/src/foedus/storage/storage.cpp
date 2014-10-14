/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage.hpp"

#include "foedus/engine.hpp"
#include "foedus/storage/storage_manager.hpp"

namespace foedus {
namespace storage {

StorageControlBlock* get_storage_control_block(Engine* engine, StorageId id) {
  return engine->get_storage_manager()->get_storage(id);
}
StorageControlBlock* get_storage_control_block(Engine* engine, const StorageName& name) {
  return engine->get_storage_manager()->get_storage(name);
}

}  // namespace storage
}  // namespace foedus
