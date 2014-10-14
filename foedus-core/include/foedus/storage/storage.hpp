/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_HPP_
#define FOEDUS_STORAGE_STORAGE_HPP_
#include <iosfwd>
#include <string>

#include "foedus/attachable.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace storage {

StorageControlBlock* get_storage_control_block(Engine* engine, StorageId id);
StorageControlBlock* get_storage_control_block(Engine* engine, const StorageName& name);

/**
 * @brief A base layout of shared data for all storage types.
 * @ingroup STORAGE
 * @details
 * Individual storage types define their own control blocks that is \e compatible with this layout.
 * @attention This is not for inheritance! Rather to guarantee the layout of 'common' part.
 * When we want to deal with a control block of unknown storage type, we reinterpret to this
 * type and obtain common information. So, the individual storage control blocks must have
 * a compatible layout to this.
 */
struct StorageControlBlock CXX11_FINAL {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  StorageControlBlock() CXX11_FUNC_DELETE;
  ~StorageControlBlock() CXX11_FUNC_DELETE;

  bool exists() const { return status_ == kExists || status_ == kMarkedForDeath; }

  void initialize() {
    status_mutex_.initialize();
    status_ = kNotExists;
    root_page_pointer_.snapshot_pointer_ = 0;
    root_page_pointer_.volatile_pointer_.word = 0;
  }
  void uninitialize() {
    status_mutex_.uninitialize();
  }

  /**
   * The mutext to protect changing the status. Reading the status is not protected,
   * so we have to make sure we don't suddenly drop a storage.
   * We first change the status to kMarkedForDeath, then drop it after a while.
   */
  soc::SharedMutex  status_mutex_;
  /** Status of the storage */
  StorageStatus     status_;
  /** Points to the root page (or something equivalent). */
  DualPagePointer   root_page_pointer_;
  /** common part of the metadata. individual storage control blocks would have derived metadata */
  Metadata          meta_;

  /** Just to make this exactly 4kb. Individual control block doesn't have this. */
  char              padding_[
    4096 - sizeof(soc::SharedMutex) - 8 - sizeof(DualPagePointer) - sizeof(Metadata)];
};

/**
 * @brief Represents one key-value store.
 * @ingroup STORAGE
 * @details
 * This pure virtual class represents a set of key-value pairs.
 * The individual storage class defines \e how they are stored.
 *
 * @par Storage vs Tables/Indexes
 * One \e storage corresponds to one set of key-value pairs.
 * A relational \e Table consists of one or more storages, primary storage and zero or more
 * secondary storages (secondary indexes).
 * However, libfoedus-core is agnostic to relational layer. It only knows about key-value store.
 * The relation between primary and secondary storages are controlled by higher layers.
 *
 * @par Instantiating Storage
 * Storage object is instantiated in two ways.
 * To newly create a storage, the user invokes storage manager's create_xxx, which instantiates
 * this object and calls create().
 * To retrieve an existing storage, bluh bluh
 *
 * @par No virtual methods
 * As storages are placed in shared memory, everything is just data bits without RTTI.
 * There is no point to use storage objects in polymorphoc way. Thus, no virtual methods.
 */
template <typename CONTROL_BLOCK>
class Storage : public Attachable<CONTROL_BLOCK> {
 public:
  Storage() : Attachable<CONTROL_BLOCK>() {}
  Storage(Engine* engine, CONTROL_BLOCK* control_block)
    : Attachable<CONTROL_BLOCK>(engine, control_block) {}
  Storage(Engine* engine, StorageControlBlock* control_block)
    : Attachable<CONTROL_BLOCK>(engine, reinterpret_cast<CONTROL_BLOCK*>(control_block)) {}
  /** Shorthand for engine->get_storage_manager()->get_storage(id) */
  Storage(Engine* engine, StorageId id)
    : Attachable<CONTROL_BLOCK>(
        engine,
        reinterpret_cast<CONTROL_BLOCK*>(get_storage_control_block(engine, id))) {}
  /** Shorthand for engine->get_storage_manager()->get_storage(name) */
  Storage(Engine* engine, const StorageName& name)
    : Attachable<CONTROL_BLOCK>(
      engine,
      reinterpret_cast<CONTROL_BLOCK*>(get_storage_control_block(engine, name))) {}

  Storage(const Storage& other)
    : Attachable<CONTROL_BLOCK>(other.engine_, other.control_block_) {}
  Storage& operator=(const Storage& other) {
    this->engine_ = other.engine_;
    this->control_block_ = other.control_block_;
    return *this;
  }

  /**
   * Returns the unique ID of this storage.
   */
  StorageId           get_id() const { return get_metadata()->id_; }

  /**
   * Returns the type of this storage.
   */
  StorageType         get_type() const { return get_metadata()->type_; }

  /**
   * Returns the unique name of this storage.
   */
  const StorageName&  get_name() const { return get_metadata()->name_; }

  /**
   * Returns the metadata of this storage.
   * @return metadata for the individual storage instance. You can dynamic_cast it to
   * derived metadata object.
   */
  const Metadata*     get_metadata() const {
    return &(reinterpret_cast<const StorageControlBlock*>(this->control_block_)->meta_);
  }

  /**
   * Returns whether this storage is already created.
   */
  bool                exists() const {
    return this->control_block_ != nullptr &&
      reinterpret_cast<const StorageControlBlock*>(this->control_block_)->exists();
  }

  // The following only defines the "interface", it's not actual virtual method
  // you can invoke in a polymorphic manner. No virtual methods in shared memory, unluckily.
  // In other words, the followings are just 'concepts', tho we don't actually define template
  // concepts which is taking forever to get into the c++ standard.

  /**
   * @brief Newly creates this storage and registers it in the storage manager.
   * @pre exists() == false
   * @param[in] metadata Metadata of this storage
   * @details
   * This is invoked from storage manager's create_xxx methods.
   * Depending on the storage type, this might take a long time to finish.
   * For a newly created storage, the instasnce of this object is an empty and
   * trivial-to-instantiate (thus no exception) until we call this method.
   */
  // ErrorStack          create(const Metadata &metadata);

  /** Drop the storage */
  // ErrorStack          drop();

  /**
   * @brief Resolves a "moved" record for a write set
   * @return whether we could track it. the only case it fails to track is the record moved
   * to deeper layers. we can also track it down to other layers, but it's rare. so, just retry
   * the whole transaction.
   * @details
   * This is the cord of the moved-bit protocol. Receiving a xct_id address that points
   * to a moved record, track the physical record in another page.
   * This method does not take lock, so it is possible that concurrent threads
   * again move the record after this.
   */
  // bool                track_moved_record(xct::WriteXctAccess *write);

  /**
   * @brief Resolves a "moved" record's xct_id only.
   * @return returns null if we couldn't track it. in that case we retry the whole transaction.
   * @details
   * This is enough for read-set verification.
   */
  // xct::LockableXctId* track_moved_record(xct::LockableXctId *address);
};


CXX11_STATIC_ASSERT(sizeof(StorageControlBlock) == 1 << 12, "StorageControlBlock is not 4kb");
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_HPP_
