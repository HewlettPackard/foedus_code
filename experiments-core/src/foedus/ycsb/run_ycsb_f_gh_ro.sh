# This is the "Experiment 1"
loggers_per_node=1
volatile_pool_size=20
snapshot_pool_size=1
reducer_buffer_size=1
duration_micro=10000000
thread_per_node=16
numa_nodes=16
log_buffer_mb=512
machine_name="GryphonHawk"
machine_shortname="gh"
fork_workers=true
null_log_device=true
high_priority=false # To set this to true, you must add "yourname - rtprio 99" to limits.conf

for hot_threshold in 0 5 256; do
  for size in 50 100 1000 10000 100000 1000000; do
    for theta in 0; do
      for addread in 10; do
        . config_ycsb_f.sh $theta $addread `expr 10 - $addread` $size $hot_threshold
        . run_common.sh
      done
    done
  done
done
