echo "FOEDUS TPC-C experiments script for $machine_shortname ($machine_name)"
echo "warehouses=$warehouses, loggers_per_node=$loggers_per_node, volatile_pool_size=$volatile_pool_size, duration_micro=$duration_micro."
echo "thread_per_node=$thread_per_node, numa_nodes=$numa_nodes, snapshot_pool_size=$snapshot_pool_size, reducer_buffer_size=$reducer_buffer_size."

payment_percents[0]=0
payment_percents[1]=15
payment_percents[2]=30
payment_percents[3]=45
payment_percents[4]=60
payment_percents[5]=75
payment_percents[6]=90
payment_percents[7]=100
payment_percents[8]=100
payment_percents[9]=100
payment_percents[10]=100

# This argument is to measure performance without logging I/O
null_log_device=true
high_priority=false # To set this to true, you must add "yourname - rtprio 99" to limits.conf
fork_workers=true

for remote_percent in 0 1 2 3 4 5 6 7 8 9 10
do
  for rep in 0 1 2
  do
    neworder_remote_percent=$remote_percent
    payment_remote_percent=${payment_percents[$remote_percent]}
    echo "NewOrder Remote-percent=$neworder_remote_percent, Payment Remote-percent=$payment_remote_percent, rep=$rep/3..."
    # be careful.
    rm -rf /dev/shm/foedus_tpcc/
    rm -rf /tmp/libfoedus.*
    sleep 5 # Linux's release of shared memory has a bit of timelag.
    echo "./tpcc -warehouses=$warehouses -fork_workers=$fork_workers -high_priority=$high_priority -null_log_device=$null_log_device -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=$neworder_remote_percent -payment_remote_percent=$payment_remote_percent -volatile_pool_size=$volatile_pool_size -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -duration_micro=$duration_micro"
    ./tpcc -warehouses=$warehouses -fork_workers=$fork_workers -high_priority=$high_priority -null_log_device=$null_log_device -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=$neworder_remote_percent -payment_remote_percent=$payment_remote_percent -volatile_pool_size=$volatile_pool_size -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -duration_micro=$duration_micro &> "result_tpcc_$machine_shortname.n$remote_percent.r$rep.log"
  done
done
