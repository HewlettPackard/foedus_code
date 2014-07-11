/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */

#include "foedus/cache/cache_hashtable.hpp"

#include <glog/logging.h>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace cache {
ErrorCode CacheHashtable::miss(
  storage::SnapshotPagePointer page_id,
  thread::Thread* context,
  storage::Page** out) {
  *out = nullptr;

  // grab a buffer page and read into it.
  /*
  memory::PagePoolOffset offset = context->get_thread_memory()->grab_free_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }
  storage::Page* new_page = cache_base_ + offset;
  CHECK_ERROR_CODE(context->read_a_snapshot_page(page_id, new_page));

  // Successfully read  it. Now, for the following accesses, let's install

  *out = new_page;
  */
  return kErrorCodeOk;
}

uint32_t determine_table_size(const memory::AlignedMemory& table_memory) {
  uint64_t buckets = table_memory.get_size() / sizeof(CacheHashtable::Bucket);
  // to speed up, leave space in neighbors of the last bucket.
  // Instead, we do not wrap-around.
  buckets -= CacheHashtable::kHopNeighbors;

  // to make the division-hashing more effective, make it prime-like.
  return assorted::generate_almost_prime_below(buckets);
}

CacheHashtable::CacheHashtable(
  const memory::AlignedMemory& table_memory,
  storage::Page* cache_base)
  : table_size_(determine_table_size(table_memory)),
  bucket_div_(table_size_),
  buckets_(reinterpret_cast<Bucket*>(table_memory.get_block())),
  cache_base_(cache_base) {
  LOG(INFO) << "Initialized CacheHashtable. node=" << table_memory.get_numa_node()
    << ", table_size=" << table_size_;
}

}  // namespace cache
}  // namespace foedus
