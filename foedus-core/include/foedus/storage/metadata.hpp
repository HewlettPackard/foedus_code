/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_METADATA_HPP_
#define FOEDUS_STORAGE_METADATA_HPP_
#include <foedus/externalize/externalizable.hpp>
#include <foedus/storage/storage_id.hpp>
#include <iosfwd>
#include <string>
namespace foedus {
namespace storage {
/**
 * @brief Metadata of one storage.
 * @ingroup STORAGE
 * @details
 * Metadata of a storage is a concise set of information about its structure, not about its data.
 * For example, ID, name, and other stuffs specific to the storage type.
 *
 * @section FORMAT Metadata file format
 * So far, we use a human-readable XML format for all metadata.
 * The main reason is ease of debugging.
 *
 * @section WRITE When metadata is written
 * Currently, all metadata of all storages are written to a single file for each snapshotting.
 * We start from previous snapshot and apply durable logs up to some epoch just like data files.
 * We have a plan to implement a stratified metadata-store equivalent to data files, but
 * it has lower priority. It happens only once per several seconds, and the cost to dump
 * that file, even in XML format, is negligible unless there are many thousands stores.
 * (yes, which might be the case later, but not for now.)
 *
 * @section READ When metadata is read
 * Snapshot metadata files are read at next snapshotting and at next restart.
 */
struct Metadata : public virtual externalize::Externalizable {
    Metadata() : id_(0), type_(INVALID_STORAGE), name_("") {}
    Metadata(StorageId id, StorageType type, const std::string& name)
        : id_(id), type_(type), name_(name) {}
    virtual ~Metadata() {}

    /**
     * Constructs an equivalent metadata object and returns a pointer to it.
     */
    virtual Metadata* duplicate() const = 0;

    /** common routine for the implementation of load() */
    ErrorStack load_base(tinyxml2::XMLElement* element);
    /** common routine for the implementation of save() */
    ErrorStack save_base(tinyxml2::XMLElement* element) const;
    /** common routine for the implementation of duplicate() */
    void duplicate_base(Metadata* duplicated) const {
        duplicated->id_ = id_;
        duplicated->type_ = type_;
        duplicated->name_ = name_;
    }

    /** the unique ID of this storage. */
    StorageId       id_;
    /** type of the storage. */
    StorageType     type_;
    /** the unique name of this storage. */
    std::string     name_;

    /** Create an instance from the given XML element, according to the type_ tag in it. */
    static Metadata* create_instance(tinyxml2::XMLElement* metadata_xml);
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_METADATA_HPP_
