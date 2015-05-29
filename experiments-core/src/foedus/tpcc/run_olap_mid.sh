# (20+20+16+6*4 + some)*4.. 500GB hugepages should be more than enough.
# sudo sh -c 'echo 250000 > /proc/sys/vm/nr_hugepages'
warehouses=24
loggers_per_node=1
volatile_pool_size=16 # HUGE. Make sure you have enough /proc/sys/vm/nr_hugepages
snapshot_pool_size=20000 # note, this one is in MB. yes, confusing...
reducer_buffer_size=20
duration_micro=30000000
thread_per_node=6
numa_nodes=4
log_buffer_mb=2048
machine_name="Mid-Low-Server"
machine_shortname="mid"
. run_olap.sh
