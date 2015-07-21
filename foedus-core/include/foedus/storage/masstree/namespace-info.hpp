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
#ifndef FOEDUS_STORAGE_MASSTREE_NAMESPACE_INFO_HPP_
#define FOEDUS_STORAGE_MASSTREE_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::storage::masstree
 * @brief \b Masstree Storage, 64-bit B-tries with internal B-trees.
 * @details
 * Although the package name says masstree [YANDONG12], we call our implementation \e Master-Tree.
 * There are a few differences from the original Masstree.
 *
 * @section MASS_FOSTER_TWIN Foster-Twin
 * See our paper (lame, yes).
 *
 * @section MASS_TERM_REMAINDER Remainder vs Remaining Key
 * Througout our code, we always use the word \b remainder rather than \b remaining-key.
 * In [YANDONG12] and [TU13], the word \e remaining-key meant the leftover of
 * the entire key after removing the common prefixes in previous B-trie layers.
 * For example, key "abcd" has remaining key "abcd" in layer-0.
 * key "abcd12345678" has remaining keys "abcd12345678" in layer-0 and "5678" in layer-1.
 * When the \e length of remaining-key is more than 8-bytes, the page also stores \e suffix,
 * which is the key part after the slice in the layer.
 * For example, "abcd12345678" stores "5678" as suffix in layer-0 while no suffix in layer-1.
 *
 * Up to here, there is no difference between \e remainder and \e remaining-key.
 * Actually, in most cases they are exactly the same.
 *
 * They differ only when a record points to next layer.
 * In the original papers/codes, the length of remaining-key is set to be 255 when
 * the record now points to next layer, and they also overwrite the area to store suffix
 * with next-page pointer.
 *
 * In Master-Tree, we always keep the remainder even when now it points to the next layer.
 * This simplifies many concurrency control issues, especially our foster-twin tracking algorithm.
 * Therefore, we never change the length of remainder. It's an immutable property of the record.
 * Instead, we have an additional mutable boolean flag "next_layer" \b in \b TID.
 * Whether the next_layer flag is ON or OFF, all threads are safe to access the remainder.
 * The commit protocol checks the flag and aborts if it's unexpectedly ON.
 *
 * This is just a subtle difference, but allthemore subtle, it's confusing!
 * Hence, we use a different word, \e remainder.
 * Below we summarize the differences:
 *
 * <table>
 * <tr><th>.</th><th>Remaining Key (Mass-Tree)</th><th>Remainder (Master-Tree)</th></tr>
 * <tr>
 *   <td>Length</td>
 *   <td>Full key length - layer*8 if not next-layer, 255 if it's now next-layer.</td>
 *   <td>Full key length - layer*8 always. Keeps the original remainder length.</td>
 * </tr>
 * <tr>
 *   <td>Suffix stored at</td>
 *   <td>The beginning of data region if not next-layer, overwritten if it's now next-layer.</td>
 *   <td>The beginning of data region always. Keeps the original remainder.</td>
 * </tr>
 * <tr>
 *   <td>Length 0xFFFF means</td>
 *   <td>The record is now next-layer, so the record has no suffix-region. \b HOWEVER,
 * concurrent threads must be careful because it was not 0xFFFF before.</td>
 *   <td>The record was initially next-layer as of creation, so the record has no suffix-region.
 * Such a record is never moved. A record that later turned to be next-layer doesn't use this value.
 * </td>
 * </tr>
 * </table>
 *
 * @section MASS_REF References
 * \li [YANDONG12] Yandong Mao, Eddie Kohler, and Robert Tappan Morris.
 *   "Cache craftiness for fast multicore key-value storage.", EuroSys, 2012.
 *
 * \li [TU13] Stephen Tu, Wenting Zheng, Eddie Kohler, Barbara Liskov, and Samuel Madden.
 *   "Speedy transactions in multicore in-memory databases.", SOSP, 2013.
 */

/**
 * @defgroup MASSTREE Masstree Storage
 * @ingroup STORAGE
 * @copydoc foedus::storage::masstree
 */

#endif  // FOEDUS_STORAGE_MASSTREE_NAMESPACE_INFO_HPP_
