# This is the "Experiment 2"
loggers_per_node=2
volatile_pool_size=16
snapshot_pool_size=1
reducer_buffer_size=1
duration_micro=10000000
thread_per_node=12
numa_nodes=4
log_buffer_mb=512
machine_name="DL580"
machine_shortname="dl580"
fork_workers=true
null_log_device=true
high_priority=false # To set this to true, you must add "yourname - rtprio 99" to limits.conf

for hot_threshold in 0 5 10 256; do
  for size in 50 100 1000 10000 100000 1000000; do
    for theta in 0; do
      for addread in 0 1 2 3 4 5 6 7 8 9 10; do
        . config_ycsb_f.sh $theta $addread `expr 10 - $addread` $size $hot_threshold
        . run_common.sh
      done
    done
  done
done
