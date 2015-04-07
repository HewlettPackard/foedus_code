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
#ifndef FOEDUS_EXTERNALIZE_NAMESPACE_INFO_HPP_
#define FOEDUS_EXTERNALIZE_NAMESPACE_INFO_HPP_
/**
 * @namespace foedus::externalize
 * @brief Object Externalization
 * @details
 * @par Overview
 * Analogous to java.io.Externalizable.
 * We externalize savepoint and configuration objects.
 * We never use them for main data files, so the code here does not have to be seriously optimized.
 *
 * @par tinyxml2 rather than boost::serialize
 * boost::serialize is not header-only.
 * Rather, we contain tinyxml2, a handy zlib-licensed library with a CMake project file.
 * This gives an added benefit of pretty XML file as the resulting file,
 * which can be hand-editted with text editor.
 * For more details of tinyxml2, see http://www.grinninglizard.com/tinyxml2/
 *
 * @par Terminology: Externalization vs Serialization
 * I just picked the word \e Externalize because \e serialization, or \e serializable, has totally
 * different meanings in DBMS and lock-free algorithm.
 */

/**
 * @defgroup EXTERNALIZE Object Externalization
 * @ingroup IDIOMS
 * @copydoc foedus::externalize
 */
#endif  // FOEDUS_EXTERNALIZE_NAMESPACE_INFO_HPP_
