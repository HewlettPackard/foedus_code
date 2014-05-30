/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_HPP_
#define FOEDUS_STORAGE_STORAGE_HPP_
#include <foedus/error_stack.hpp>
#include <foedus/initializable.hpp>
#include <foedus/storage/storage_id.hpp>
#include <foedus/storage/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <iosfwd>
#include <string>
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
    virtual const std::string&  get_name() const = 0;

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

    /** Just delegates to describe(). */
    friend std::ostream& operator<<(std::ostream& o, const Storage& v);
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_HPP_
