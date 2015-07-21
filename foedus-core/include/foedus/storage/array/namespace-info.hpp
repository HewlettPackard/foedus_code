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
#ifndef FOEDUS_STORAGE_ARRAY_NAMESPACE_INFO_HPP_
#define FOEDUS_STORAGE_ARRAY_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::storage::array
 * @brief \b Array Storage, a dense and regular array.
 * @details
 * @section ARRAY_BASICS Basic Structure
 * Our array-storage is an extremely simple data structure at the cost
 * of limited applicability (fix-sized, regular, dense arrays).
 * The page layout has little per-page information that is dynamic, meaning that
 * most data never changes after the array storage is created.
 * Further, as everything is pre-allocated, no phantoms, no insertion/splits, nor anything complex.
 * Thus, little synchronization hassles.
 *
 * @section ARRAY_OPS Supported Operations
 * Array storage allows very few data operations.
 * \li \b Reads a record or a range of records.
 * \li \b Overwrites a record.
 * \li Reads a \b range of records in batched fashion. (Not implemented so far, just loop over it.
 * This might be useful for a big array of really small records to avoid function call overheads.)
 *
 * In other words, the following operations are \b NOT supported.
 * \li \b Inserts or \b Deletes a record because all records always exist in an array storage
 * (dense array). Client programs can achieve the same thing by storing one-bit in
 * payload as a deletion-flag and overwrites it.
 * \li \b Resize individual records or the entire array. All records are fix-sized and
 * pre-allocated when the array storage is initialized.
 *
 * If these limitations do not cause an issue, array storage is a best choice as it's extremely
 * efficient and scalable thanks to the simplicity.
 *
 * @section ARRAY_HIE Page Hierarchy
 * Despite the name of this storage type, we do have a page hierarchy, which is required to handle
 * switches between volatile/snapshot pages. Hence, a more precise description of this storage type
 * is a fix-sized pre-allocated \e tree that has only integer-keys from 0 to array_size-1.
 *
 * All data (payload) are stored in leaf pages and interior pages contain only pointers to its
 * children. There is only one root page per array, which may or may not be a leaf page.
 * Just like B-trees in many sense.
 *
 * @section ARRAY_LAYOUT Page Layout
 * @par Header and Data
 * <table>
 *  <tr><th>Fix-Sized HEADER (kHeaderSize bytes)</th><td>DATA</td></tr>
 * </table>
 *
 * @par Interior Node Data
 * <table>
 *  <tr><th>InteriorRecord</th><td>InteriorRecord</td><th>...</th></tr>
 * </table>
 * See foedus::storage::array::ArrayPage#InteriorRecord.
 *
 * @par Leaf Node Data
 * <table>
 *  <tr><th>Record</th><td>Padding</td><th>Record</th><td>Padding</td><th>...</th></tr>
 * </table>
 * We simply puts foedus::storage::Record contiguously, but with a padding (0-7 bytes).
 * to make sure Record are 8-byte aligned because we do atomic operations on Record's owner_id_.
 *
 * @section ARRAY_VER Concurrency Control
 * We do a bit special concurrency control for this storage type.
 * Because everything is pre-allocated and no split/physical-delete/merge whatsoever,
 * we can do the ModCount concurrency control \b per \b Record, not per page.
 * We store ModCount for each record, instead stores no ModCount in page header.
 * Thus, individual transactions that update records in same page have no contention except they
 * update the exact same record. We even don't track node-set; only record-set is checked at commit.
 *
 * This simplifies and increases concurrency for most data accesses, but we have to be careful when
 * the page eviction thread drops the volatile page.
 * We need to know the \b largest Epoch of records in the page at the point, but concurrent
 * transactions might be modifying records at the same time. Thus, we do two-path optimistic
 * concurrency control similar to our commit protocol for \ref MASSTREE.
 *  \li The eviction thread takes a look at all Epoch in the page, remembering the largest one.
 *  \li Fence
 *  \li Then, it puts a mark on the pointer from the parent page to the evicted page to announce
 * that the volatile page is about to be evicted.
 *  \li Fence
 *  \li Finally, it takes a look at all Epoch in the page again and completes the eviction
 * only if it sees the same largest Epoch. If not, gives up (because the volatile page is
 * now newer, no chance to drop it with the latest snapshot version).
 *
 * @section ARRAY_REF References
 * \li [SCIDB] Cudre-Mauroux, P. and Kimura, H. and Lim, K.-T. and Rogers, J. and Simakov, R. and
 *   Soroush, E. and Velikhov, P. and Wang, D. L. and Balazinska, M. and Becla, J. and DeWitt, D.
 *   and Heath, B. and Maier, D. and Madden, S. and Patel, J. and Stonebraker, M. and Zdonik, S.
 *   "A demonstration of SciDB: a science-oriented DBMS.", VLDB, 2009.
 */

/**
 * @defgroup ARRAY Array Storage
 * @ingroup STORAGE
 * @copydoc foedus::storage::array
 */

#endif  // FOEDUS_STORAGE_ARRAY_NAMESPACE_INFO_HPP_
