# This is the "Experiment 2"
loggers_per_node=1
volatile_pool_size=2
snapshot_pool_size=1
reducer_buffer_size=1
duration_micro=10000000
thread_per_node=6
numa_nodes=2
log_buffer_mb=128
machine_name="Z820"
machine_shortname="z820"
fork_workers=true
null_log_device=true
high_priority=false # To set this to true, you must add "yourname - rtprio 99" to limits.conf

. run_ycsb_s_common.sh
