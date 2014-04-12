/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_HPP_
#define FOEDUS_STORAGE_STORAGE_HPP_
#include <foedus/initializable.hpp>
#include <foedus/storage/storage_id.hpp>
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
 * Storage object is
 */
class Storage : public virtual Initializable {
 public:
    virtual ~Storage() {}

    virtual StorageId get_storage_id() const = 0;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_HPP_
