/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_HPP_
#define FOEDUS_STORAGE_STORAGE_HPP_
#include <iosfwd>
#include <string>

#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/storage/fwd.hpp"
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
class Storage : public virtual Initializable {
 public:
  virtual ~Storage() {}

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
   * @details
   * This is invoked from storage manager's create_xxx methods.
   * Depending on the storage type, this might take a long time to finish.
   * For a newly created storage, the instasnce of this object is an empty and
   * trivial-to-instantiate (thus no exception) until we call this method.
   */
  virtual ErrorStack          create(thread::Thread* context) = 0;

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
 * @brief Interface to instantiate a storage.
 * @ingroup STORAGE
 * @details
 * This is an interface of factory classes for storage classes.
 * One reason to have a factory class in this case is to encapsulate error handling during
 * instantiation, which is impossible if we simply invoke C++ constructors.
 */
class StorageFactory {
 public:
  virtual ~StorageFactory() {}

  /**
   * Returns the type of storages this factory creates.
   */
  virtual StorageType  get_type() const = 0;

  /**
   * @brief Tells if the given metadata object satisfies the requirement of the storage.
   * @param[in] metadata metadata object of a derived class
   * @details
   * For example, ArrayStorageFactory receive only ArrayMetadata.
   * The storage manager checks with all storage factories for each instantiation request
   * to identify the right factory class (a bit wasteful, but storage creation is a rare event).
   */
  virtual bool is_right_metadata(const Metadata *metadata) const = 0;

  /**
   * @brief Instantiate a storage object with the given metadata.
   * @param[in] engine database engine
   * @param[in] metadata metadata of the newly instantiated storage object
   * @param[out] storage set only when this method succeeds. otherwise null.
   * @pre is_right_metadata(metadata)
   * @details
   * This method verifies the metadata object and might return errors for various reasons.
   */
  virtual ErrorStack get_instance(Engine* engine, const Metadata *metadata,
                                  Storage** storage) const = 0;

  /**
   * Adds a log entry for newly creating the storage to the context's log buffer.
   * @pre is_right_metadata(metadata)
   */
  virtual void add_create_log(const Metadata *metadata, thread::Thread* context) const = 0;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_HPP_
