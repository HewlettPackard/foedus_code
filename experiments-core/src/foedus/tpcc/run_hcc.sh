echo "FOEDUS TPC-C experiments script with HCC for $machine_shortname ($machine_name)"
echo "warehouses=$warehouses, loggers_per_node=$loggers_per_node, volatile_pool_size=$volatile_pool_size, duration_micro=$duration_micro."
echo "thread_per_node=$thread_per_node, numa_nodes=$numa_nodes, snapshot_pool_size=$snapshot_pool_size, reducer_buffer_size=$reducer_buffer_size."

null_log_device=true # Without logging I/O
high_priority=false # To set this to true, you must add "yourname - rtprio 99" to limits.conf
fork_workers=true

for hcc_policy in 0 1 2
do
  for rep in 0 1 2 3 4 5 6 7 8 9
  do
    echo "hcc_policy=$hcc_policy, rep=$rep/10..."
    # be careful.
    rm -rf /dev/shm/foedus_tpcc/
    rm -rf /tmp/libfoedus.*
    sleep 5 # Linux's release of shared memory has a bit of timelag.
    export CPUPROFILE_FREQUENCY=1 # https://code.google.com/p/gperftools/issues/detail?id=133
    echo "./tpcc -warehouses=$warehouses -fork_workers=$fork_workers -nvm_folder=/dev/shm -high_priority=$high_priority -null_log_device=$null_log_device -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=1 -payment_remote_percent=15 -volatile_pool_size=$volatile_pool_size -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -duration_micro=$duration_micro -hcc_policy=$hcc_policy"
    env CPUPROFILE_FREQUENCY=1 ./tpcc -warehouses=$warehouses -take_snapshot=false -fork_workers=$fork_workers -nvm_folder=/dev/shm -high_priority=$high_priority -null_log_device=$null_log_device -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=1 -payment_remote_percent=15 -volatile_pool_size=$volatile_pool_size -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -duration_micro=$duration_micro -hcc_policy=$hcc_policy &> "result_tpcc_hcc_$machine_shortname.h$hcc_policy.r$rep.log"
  done
done
