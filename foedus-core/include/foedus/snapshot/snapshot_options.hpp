/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <foedus/fs/device_emulation_options.hpp>
#include <iosfwd>
#include <string>
#include <vector>
namespace foedus {
namespace snapshot {
/**
 * @brief Set of options for snapshot manager.
 * @ingroup SNAPSHOT
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct SnapshotOptions CXX11_FINAL : public virtual externalize::Externalizable {
    /**
     * Constructs option values with default values.
     */
    SnapshotOptions();

    /**
     * @brief Folder paths of snapshot folders.
     * @details
     * The folders may or may not be on different physical devices.
     * The snapshot folders are used in round-robbin fashion.
     * @attention The default value is just one entry of current folder. When you modify this
     * setting, do NOT forget removing the default entry; call folder_paths_.clear() first.
     */
    std::vector<std::string>            folder_paths_;

    /** Settings to emulate slower data device. */
    foedus::fs::DeviceEmulationOptions  emulation_;

    ErrorStack load(tinyxml2::XMLElement* element) CXX11_OVERRIDE;
    ErrorStack save(tinyxml2::XMLElement* element) const CXX11_OVERRIDE;
    friend std::ostream& operator<<(std::ostream& o, const SnapshotOptions& v) {
        v.save_to_stream(&o);
        return o;
    }
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
