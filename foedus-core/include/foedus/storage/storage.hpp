/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_HPP_
#define FOEDUS_STORAGE_STORAGE_HPP_
#include <iosfwd>
#include <string>

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
/**
 * @brief Represents one key-value store.
 * @ingroup STORAGE
 * @details
 * This pure virtual class represents a set of key-value pairs.
 * The derived class defines \e how they are stored.
 *
 * @section ST Storage vs Tables/Indexes
 * One \e storage corresponds to one set of key-value pairs.
 * A relational \e Table consists of one or more storages, primary storage and zero or more
 * secondary storages (secondary indexes).
 * However, libfoedus-core is agnostic to relational layer. It only knows about key-value store.
 * The relation between primary and secondary storages are controlled by higher layers.
 *
 * @section INSTANTIATE Instantiating Storage
 * Storage object is instantiated in two ways.
 * To newly create a storage, the user invokes storage manager's create_xxx, which instantiates
 * this object and calls create().
 * To retrieve an existing storage, bluh bluh
 */
class Storage {
 public:
  /**
   * Returns the unique ID of this storage.
   */
  virtual StorageId           get_id() const = 0;

  /**
   * Returns the type of this storage.
   */
  virtual StorageType         get_type() const = 0;

  /**
   * Returns the unique name of this storage.
   */
  virtual const StorageName&  get_name() const = 0;

  /**
   * Returns the metadata of this storage.
   * @return metadata for the individual storage instance. You can dynamic_cast it to
   * derived metadata object.
   */
  virtual const Metadata*     get_metadata() const = 0;

  /**
   * Returns whether this storage is already created.
   */
  virtual bool                exists() const = 0;

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
  virtual ErrorStack          create(const Metadata &metadata) = 0;

  virtual ErrorStack          drop() = 0;

  /**
   * Implementation of ostream operator.
   */
  virtual void                describe(std::ostream* o) const = 0;

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
  virtual bool                track_moved_record(xct::WriteXctAccess *write) = 0;

  /**
   * @brief Resolves a "moved" record's xct_id only.
   * @return returns null if we couldn't track it. in that case we retry the whole transaction.
   * @details
   * This is enough for read-set verification.
   */
  virtual xct::LockableXctId* track_moved_record(xct::LockableXctId *address) = 0;

  /** Just delegates to describe(). */
  friend std::ostream& operator<<(std::ostream& o, const Storage& v);
};

/**
 * A base layout of shared data for all storage types.
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

CXX11_STATIC_ASSERT(sizeof(StorageControlBlock) == 1 << 12, "StorageControlBlock is not 4kb");
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_HPP_
