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
#ifndef FOEDUS_STORAGE_HASH_HASH_COMBO_HPP_
#define FOEDUS_STORAGE_HASH_HASH_COMBO_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_id.hpp"

namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief A set of information that are used in many places, extracted from the given key.
 * @ingroup HASH
 * @details
 * These are just "usual" combo, and of course we occasionally need only some of them.
 * In such a place, constructing this object is a waste, so be careful.
 * This is a POD, assuming key_ points to an immutable place.
 * Also, header-only except ostream.
 */
struct HashCombo {
  HashValue               hash_;
  HashBin                 bin_;
  BloomFilterFingerprint  fingerprint_;
  IntermediateRoute       route_;
  /**
   * TASK(Hideaki) it might cause more harm than good to have this member.
   * The caller must be careful on the lifetime of the pointee.
   * We might exclude this one later. Then this object really becomes an independent value-object.
   */
  const char*             key_;
  uint16_t                key_length_;

  HashCombo(const char* key, uint16_t key_length, const HashMetadata& meta);

  friend std::ostream& operator<<(std::ostream& o, const HashCombo& v);
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_COMBO_HPP_
