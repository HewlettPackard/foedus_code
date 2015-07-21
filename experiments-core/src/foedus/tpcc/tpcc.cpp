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
/**
 * @file foedus/tpcc/tpcc.cpp
 * @brief TPC-C experiment
 * @author kimurhid
 * @date 2014/08/01
 * @details
 * This implements the popular TPC-C benchmark.
 *
 * @section ENVIRONMENTS Environments
 *
 * @section OTHER Other notes
 *
 * @section RESULTS Latest Results
 */
#include "foedus/tpcc/tpcc.hpp"

#include "foedus/tpcc/tpcc_driver.hpp"

int main(int argc, char **argv) {
  return foedus::tpcc::driver_main(argc, argv);
}
