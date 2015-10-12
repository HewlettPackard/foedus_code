
Data Generator
------------
- Node property: (uint64_t node_id, uint32_t x, uint32_t y)
- Edge property: (uint64_t from_node, uint64_t to_node, uint32_t miles)

There are a few sizing parameters:
 - miles_fluke
 - node_count
 - edge_max
 - edge_prob

miles(node-1, node-2) is the actual mileage on the road to travel from node-1 to node-2.
  miles(node-1, node-2)
    := random[1, 1 + miles_fluke] * sqrt((x1-x2)^2 + (y1-y2)^2)

miles_fluke is 0 to arbitrary number. probably 1 or something, to represent non-straight or bumpy/inefficient roads.

node_count is the number of nodes.
Their x/y are randomly generated.

edge_max/edge_prob define the topology.
For each node-z, we consider edge_max nodes whose coordinates are close to node-z's.
For each of them, we randomly instantiate them as edges for edge_prob probability.
Edges are uni-directional to make the path-finding more interesting.

Considering edge_max=20/edge_prob=0.8 or something.

All randoms are uniform random. Can use skewed randoms, but I don't think it's important in the main story.

Queries
------------
Because of how I generated the data, Node IDs are strongly correlated with their coordinates.

For pair-wise navigational queries, we just pick two nodes close to each other.

  from_id : randomly pick from [0, #nodes)
  to_id : from_id + 64 (or 128, 256,... the larger, the more steps)

Most likely all node-pairs are reachable (20 edges), but there is a small chance that some node-pairs are unreachable.


For analytic queries, just pick any node.
 from_id : randomly pick from [0, #nodes)

From that node, we will calculate all-destination shortest-paths.

For more details about the query exectuion, see foedus::sssp::SsspDriver.
