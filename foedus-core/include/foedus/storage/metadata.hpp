/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_STORAGE_METADATA_HPP_
#define FOEDUS_STORAGE_METADATA_HPP_
#include <iosfwd>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/assorted/fixed_string.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
/**
 * @brief Metadata of one storage.
 * @ingroup STORAGE
 * @details
 * Metadata of a storage is a concise set of information about its structure, not about its data.
 * For example, ID, name, and other stuffs specific to the storage type.
 *
 * @par Metadata file format
 * So far, we use a human-readable XML format for all metadata.
 * The main reason is ease of debugging.
 *
 * @par When metadata is written
 * Currently, all metadata of all storages are written to a single file for each snapshotting.
 * We start from previous snapshot and apply durable logs up to some epoch just like data files.
 * We have a plan to implement a stratified metadata-store equivalent to data files, but
 * it has lower priority. It happens only once per several seconds, and the cost to dump
 * that file, even in XML format, is negligible unless there are many thousands stores.
 * (yes, which might be the case later, but not for now.)
 *
 * @par When metadata is read
 * Snapshot metadata files are read at next snapshotting and at next restart.
 *
 * @par No virtual methods and no heap-allocation
 * Metadata objects are placed in shared memory.
 * It must not have any heap-allocated data (eg std::string) nor virtual methods.
 */
struct Metadata {
  /** Tuning parameters related to snapshotting. */
  struct SnapshotThresholds {
    SnapshotThresholds() : snapshot_trigger_threshold_(0), snapshot_keep_threshold_(0) {}
    /**
     * [Not implemented yet] If this storage has more than this number of volatile pages,
     * log gleaner will soon start to drop some of the volatile pages.
     * Default is 0, meaning this storage has no such threshold.
     */
    uint32_t        snapshot_trigger_threshold_;
    /**
     * [Only partially implemented] When a log gleaner created new snapshot pages for this storage,
     * this storage tries to keep this number of volatile pages. The implementation will try to
     * keep volatile pages that will most frequently used.
     * Default is 0, meaning this storage drops all unmodified volatile pages after each snapshot.
     * Each storage type provides additional knobs specific to their structure.
     * Checkout the derived metadata class.
     */
    uint32_t        snapshot_keep_threshold_;
  };

  Metadata()
    : id_(0), type_(kInvalidStorage), name_(""), root_snapshot_page_id_(0), snapshot_thresholds_() {
  }
  Metadata(StorageId id, StorageType type, const StorageName& name)
    : id_(id), type_(type), name_(name), root_snapshot_page_id_(0), snapshot_thresholds_() {}
  Metadata(
    StorageId id,
    StorageType type,
    const StorageName& name,
    SnapshotPagePointer root_snapshot_page_id)
    : id_(id),
    type_(type),
    name_(name),
    root_snapshot_page_id_(root_snapshot_page_id),
    snapshot_thresholds_() {}

  /** to_string operator of all Metadata objects. */
  static std::string describe(const Metadata& metadata);

  bool keeps_all_volatile_pages() const {
    return snapshot_thresholds_.snapshot_keep_threshold_ == 0xFFFFFFFFU;
  }

  /** the unique ID of this storage. */
  StorageId       id_;
  /** type of the storage. */
  StorageType     type_;
  /** the unique name of this storage. */
  StorageName     name_;
  /**
   * Pointer to a snapshotted page this storage is rooted at.
   * This is 0 until this storage has the first snapshot.
   */
  SnapshotPagePointer root_snapshot_page_id_;

  SnapshotThresholds  snapshot_thresholds_;
};

struct MetadataSerializer : public virtual externalize::Externalizable {
  MetadataSerializer() : data_(CXX11_NULLPTR) {}
  explicit MetadataSerializer(Metadata *data) : data_(data) {}
  virtual ~MetadataSerializer() {}

  /** common routine for the implementation of load() */
  ErrorStack load_base(tinyxml2::XMLElement* element);
  /** common routine for the implementation of save() */
  ErrorStack save_base(tinyxml2::XMLElement* element) const;

  static ErrorStack load_all_storages_from_xml(
    storage::StorageId largest_storage_id,
    tinyxml2::XMLElement* element,
    StorageControlBlock* blocks);
  static ErrorStack save_all_storages_to_xml(
    storage::StorageId largest_storage_id,
    tinyxml2::XMLElement* element,
    StorageControlBlock* blocks);

  Metadata *data_;
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_METADATA_HPP_
