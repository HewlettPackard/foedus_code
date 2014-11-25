/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_page_impl.hpp"

#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {
void HashRootPage::initialize_volatile_page(
  StorageId storage_id,
  VolatilePagePointer page_id,
  const HashRootPage* parent,
  uint64_t begin_bin,
  uint64_t end_bin) {
  std::memset(this, 0, kPageSize);
  header_.init_volatile(page_id, storage_id, kHashRootPageType);
  begin_bin_ = begin_bin;
  end_bin_ = end_bin;
  if (parent) {
    ASSERT_ND(begin_bin >= parent->get_begin_bin());
    ASSERT_ND(begin_bin < parent->get_end_bin());
    ASSERT_ND(end_bin > parent->get_begin_bin());
    ASSERT_ND(end_bin <= parent->get_end_bin());
  }
}

void HashBinPage::initialize_volatile_page(
  StorageId storage_id,
  VolatilePagePointer page_id,
  const HashRootPage* parent,
  uint64_t begin_bin,
  uint64_t end_bin) {
  std::memset(this, 0, kPageSize);
  header_.init_volatile(page_id, storage_id, kHashBinPageType);
  begin_bin_ = begin_bin;
  end_bin_ = end_bin;
  ASSERT_ND(parent);
  ASSERT_ND(begin_bin >= parent->get_begin_bin());
  ASSERT_ND(begin_bin < parent->get_end_bin());
  ASSERT_ND(end_bin > parent->get_begin_bin());
  ASSERT_ND(end_bin <= parent->get_end_bin());
}

void HashDataPage::initialize_volatile_page(
  StorageId storage_id,
  VolatilePagePointer page_id,
  const Page* parent,
  uint64_t bin) {
  std::memset(this, 0, kPageSize);
  header_.init_volatile(page_id, storage_id, kHashDataPageType);
  set_bin(bin);
  ASSERT_ND(parent);
  if (parent->get_header().get_page_type() == kHashBinPageType) {
    const HashBinPage* parent_casted = reinterpret_cast<const HashBinPage*>(parent);
    parent_casted->assert_bin(bin);
  } else {
    const HashDataPage* parent_casted = reinterpret_cast<const HashDataPage*>(parent);
    ASSERT_ND(parent_casted->get_bin() == bin);
  }
}

void hash_bin_volatile_page_init(const VolatilePageInitArguments& args) {
  ASSERT_ND(args.parent_);
  ASSERT_ND(args.page_);
  ASSERT_ND(args.index_in_parent_ < kHashRootPageFanout);
  StorageId storage_id = args.parent_->get_header().storage_id_;
  HashBinPage* page = reinterpret_cast<HashBinPage*>(args.page_);

  ASSERT_ND(args.parent_->get_header().get_page_type() == kHashRootPageType);
  const HashRootPage* parent = reinterpret_cast<const HashRootPage*>(args.parent_);

  uint64_t begin_bin = parent->get_begin_bin() + args.index_in_parent_ * kBinsPerPage;
  uint64_t end_bin = begin_bin + kBinsPerPage;
  if (end_bin > parent->get_end_bin()) {
    // This must mean the parent page and this page are right-most.
    end_bin = parent->get_end_bin();
    ASSERT_ND(end_bin
      == HashStorage(args.context_->get_engine(), storage_id).get_hash_metadata()->get_bin_count());
  }
  page->initialize_volatile_page(storage_id, args.page_id, parent, begin_bin, end_bin);
}

void hash_data_volatile_page_init(const VolatilePageInitArguments& args) {
  ASSERT_ND(args.parent_);
  ASSERT_ND(args.page_);
  StorageId storage_id = args.parent_->get_header().storage_id_;
  HashDataPage* page = reinterpret_cast<HashDataPage*>(args.page_);
  PageType parent_type = args.parent_->get_header().get_page_type();
  uint64_t bin;
  if (parent_type == kHashBinPageType) {
    const HashBinPage* parent = reinterpret_cast<const HashBinPage*>(args.parent_);
    ASSERT_ND(args.index_in_parent_ + parent->get_begin_bin() < parent->get_end_bin());
    bin = parent->get_begin_bin() + args.index_in_parent_;
  } else {
    ASSERT_ND(parent_type == kHashDataPageType);
    const HashDataPage* parent = reinterpret_cast<const HashDataPage*>(args.parent_);
    bin = parent->get_bin();
  }
  page->initialize_volatile_page(storage_id, args.page_id, args.parent_, bin);
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
