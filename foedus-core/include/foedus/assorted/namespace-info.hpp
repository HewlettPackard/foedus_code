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
#ifndef FOEDUS_ASSORTED_NAMESPACE_INFO_HPP_
#define FOEDUS_ASSORTED_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::assorted
 * @brief Assorted Methods/Classes that are too subtle to have their own packages.
 * @details
 * Do \b NOT use this package to hold hundleds of classes/methods. That's a class design failure.
 * This package should contain really only a few methods and classes, each of which should be
 * extremely simple and unrelated each other. Otherwise, make a package for them.
 * Learn from the stupid history of java.util.
 */

/**
 * @defgroup ASSORTED Assorted Methods/Classes
 * @ingroup IDIOMS
 * @copydoc foedus::assorted
 */

#endif  // FOEDUS_ASSORTED_NAMESPACE_INFO_HPP_
