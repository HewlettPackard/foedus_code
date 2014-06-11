/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/metadata.hpp>
#include <foedus/storage/array/array_metadata.hpp>
#include <glog/logging.h>
#include <tinyxml2.h>
namespace foedus {
namespace storage {
ErrorStack Metadata::load_base(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, id_);
    EXTERNALIZE_LOAD_ENUM_ELEMENT(element, type_);
    EXTERNALIZE_LOAD_ELEMENT(element, name_);
    return RET_OK;
}

ErrorStack Metadata::save_base(tinyxml2::XMLElement* element) const {
    EXTERNALIZE_SAVE_ELEMENT(element, id_, "");
    EXTERNALIZE_SAVE_ENUM_ELEMENT(element, type_, "");
    EXTERNALIZE_SAVE_ELEMENT(element, name_, "");
    return RET_OK;
}

Metadata* Metadata::create_instance(tinyxml2::XMLElement* metadata_xml) {
    StorageType type = static_cast<StorageType>(metadata_xml->IntAttribute("type_"));
    // if any error happens in tinyxml2, it returns 0, which is INVALID_STORAGE
    switch (type) {
        case ARRAY_STORAGE:
            return new array::ArrayMetadata();
        case HASH_STORAGE:
        case MASSTREE_STORAGE:
        case SEQUENTIAL_STORAGE:
            // TODO(Hideaki): Implement
        default:
            LOG(FATAL) << "Unexpected storage type:" << type;
    }
    return nullptr;
}

}  // namespace storage
}  // namespace foedus
