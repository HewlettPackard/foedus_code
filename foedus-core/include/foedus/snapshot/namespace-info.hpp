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
#ifndef FOEDUS_SNAPSHOT_NAMESPACE_INFO_HPP_
#define FOEDUS_SNAPSHOT_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::snapshot
 * @brief \b Snapshot \b Manager, which manages snapshot files of the database.
 * @details
 * This package contains classes to handle \e snapshot files.
 *
 * @section SP Snapshot
 * One snapshot consists of a set of snapshot files and a snapshot metadata file (snapshot.xml).
 * Every snapshot is tagged with \e base epoch and \e valid-until epoch.
 * The snapshot \e logically contains all information of the database upto valid-until epoch,
 * meaning the previous snapshot (whose valid-until should be equal to base epoch of the snapshot)
 * as well as all log entries in the transactional log from a base epoch
 * until valid-until epoch.
 *
 * @section SPFiles Snapshot Files
 * However, the snapshot does not necessarily \e physically contains all information from the
 * previous snapshot because it makes each snapshot-ing too expensive.
 * One common approach is LSM-Tree (Log Structured Merge Tree), but it is not a good fit for
 * serializable transactional processing. Even a trivial primary key constraint would be too
 * expensive on top of LSM-Tree.
 *
 * Instead, snapshot files in FOEDUS are \b overlays of the database image.
 * Each snapshot file contains new version of data pages that \b overwrite
 * a required portion of storages. They are not incremental new-tuples/tombstones data as in LSM.
 * They are a complete representation of the storages, but it might contain pointers to old snapshot
 * files if pages under it had no change.
 *
 * In the worst case, transactions in one epoch updates just one tuple in every page,
 * resulting in a snapshot that physically contains all data. However, it is rare and such a
 * workload is fundamentally expensive if data size does not fit DRAM (if it does, this approach
 * is also fine).
 *
 * @section SNAPSHOTTING Making a new Snapshot
 * Snapshot Manager creates a new set of snapshot files as well as its metadata file occasionally.
 * The frequency is a tuning knob.
 * The mechanism to create snapshot files is called \b Log-Gleaner (foedus::snapshot::LogGleaner).
 * See its documentation below.
 *
 * @copydetails foedus::snapshot::LogGleaner
 */

/**
 * @defgroup SNAPSHOT Snapshot Manager
 * @ingroup COMPONENTS
 * @copydoc foedus::snapshot
 */

#endif  // FOEDUS_SNAPSHOT_NAMESPACE_INFO_HPP_
