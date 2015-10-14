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
#ifndef FOEDUS_EXPERIMENTS_SSSP_MINHEAP_HPP_
#define FOEDUS_EXPERIMENTS_SSSP_MINHEAP_HPP_

#include <stdint.h>

#include <iosfwd>
#include <queue>
#include <vector>

#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/sssp/sssp_common.hpp"

namespace foedus {
namespace sssp {

struct DijkstraEntry {
  DijkstraEntry(uint32_t distance, NodeId node_id)
    : distance_(distance), node_id_(node_id) {
  }
  uint32_t distance_;
  NodeId node_id_;

  inline bool operator<(const DijkstraEntry& entry) const {
    // NOTE: std::priority_queue is a *max*-heap. To use it as a min-heap,
    // we reverse the comparison semantics here.
    return distance_ > entry.distance_;
  }
};

/**
 * Derive to hijack "c", which is protected.
 * We need clear() and reserve().
 */
struct HijackedStlQueue
  : public std::priority_queue< DijkstraEntry, std::vector<DijkstraEntry> > {
  HijackedStlQueue() : std::priority_queue< DijkstraEntry, std::vector<DijkstraEntry> >() {
  }
  void clear() {
    c.clear();
  }
  void reserve(uint64_t capacity) {
    c.reserve(capacity);
  }
};

/**
 * @brief An in-memory single-threaded minheap for Dijkstra.
 * @details
 * STL's priority_queue doesn't support decrease_priority functionality,
 * so we do not bother deleting existing records.
 * Instead, whenever we pop, we check against the shortest-distance in the hashtable.
 * If the entry is not as short as the one in the hashtable, we ignore the enty.
 *
 * Relevant readings:
 * \li http://en.cppreference.com/w/cpp/container/priority_queue
 * \li http://stackoverflow.com/questions/9209323/easiest-way-of-using-min-priority-queue-with-key-update-in-c
 * \li http://stackoverflow.com/questions/2852140/priority-queue-clear-method
 */
class DijkstraMinheap final {
 public:
  void      clean() { queue_.clear(); }
  void      reserve(uint64_t capacity) { queue_.reserve(capacity); }
  DijkstraEntry pop() ALWAYS_INLINE;
  void      push(DijkstraEntry entry) ALWAYS_INLINE;

  friend std::ostream& operator<<(std::ostream& o, const DijkstraMinheap& v);

 private:
  HijackedStlQueue  queue_;
};

inline DijkstraEntry DijkstraMinheap::pop() {
  DijkstraEntry ret = queue_.top();
  queue_.pop();
  return ret;
}

inline void DijkstraMinheap::push(DijkstraEntry entry) { queue_.push(entry); }

}  // namespace sssp
}  // namespace foedus
#endif  // FOEDUS_EXPERIMENTS_SSSP_MINHEAP_HPP_
