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
#ifndef FOEDUS_EXPERIMENTS_SSSP_DRIVER_HPP_
#define FOEDUS_EXPERIMENTS_SSSP_DRIVER_HPP_

#include "foedus/sssp/sssp_common.hpp"

namespace foedus {
namespace sssp {

/**
 * @brief Minimalistic query execution engine for the road-like graph microbench.
 * @details
 * This query engine supports only two types of queries.
 * \li Single-pair shortest path, or a navigational query, which is optimized for throughput.
 * \li Single-source (all-destinations) shortest path, or SSSP, which is optimized for latency.
 *
 * For navigational queries, this engine runs many of them concurrently on about half of
 * worker threads. Each query is short and runs only on one worker thread.
 * We run thousands of them per second. Currently we use the good old Dijkstra.
 *
 * For SSSP, this engine runs just one query at a time, but instead it parallelizes
 * the query on about half of worker threads. For parallelization,
 * we use Xxx algorithm.
 */
struct SsspDriver {
  int main_impl(int argc, char **argv);
};


}  // namespace sssp
}  // namespace foedus

#endif  // FOEDUS_EXPERIMENTS_SSSP_DRIVER_HPP_
