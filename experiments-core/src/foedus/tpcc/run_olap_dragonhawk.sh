# Do NOT use all 15 cores per node. We observed that redhat kernel jumps in and messes up.
# Leave the OS fair chunk. This wasn't an issue in DL580/Z820. not sure why.
warehouses=192
loggers_per_node=2
volatile_pool_size=32 # HUGE. Make sure you have enough /proc/sys/vm/nr_hugepages
snapshot_pool_size=40000 # note, this one is in MB. yes, confusing...
reducer_buffer_size=80
duration_micro=30000000
thread_per_node=12
numa_nodes=16
log_buffer_mb=2048
machine_name="DragonHawk"
machine_shortname="dh"
. run_olap.sh
