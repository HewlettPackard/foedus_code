# Do NOT use all 15 cores per node. We observed that redhat kernel jumps in and messes up.
# Leave the OS fair chunk. This wasn't an issue in DL580/Z820. not sure why.
warehouses=192
loggers_per_node=2
volatile_pool_size=64
duration_micro=10000000
thread_per_node=12
numa_nodes=16
log_buffer_mb=1024
machine_name="DragonHawk"
machine_shortname="dh"
. run_common.sh
