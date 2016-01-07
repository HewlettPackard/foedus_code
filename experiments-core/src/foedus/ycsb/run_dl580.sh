loggers_per_node=2
volatile_pool_size=16
snapshot_pool_size=2
reducer_buffer_size=1
duration_micro=10000000
thread_per_node=12
numa_nodes=4
log_buffer_mb=1024
machine_name="DL580"
machine_shortname="dl580"
fork_workers=true
null_log_device=true
high_priority=false # To set this to true, you must add "yourname - rtprio 99" to limits.conf

. config_ycsb.sh
. run_common.sh
