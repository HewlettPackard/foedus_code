echo "FOEDUS TPC-C NVM experiments script for $machine_shortname ($machine_name)"
echo "warehouses=$warehouses, loggers_per_node=$loggers_per_node, volatile_pool_size=$volatile_pool_size, duration_micro=$duration_micro."
echo "thread_per_node=$thread_per_node, numa_nodes=$numa_nodes, volatile_pool_size=$volatile_pool_size, duration_micro=$duration_micro."

high_priority=false # To set this to true, you must add "yourname - rtprio 99" to limits.conf
fork_workers=true

# remote fraction is fixed to 1 (default TPC-C)
# instead, this experiment varies NVM latency to emulate

for nvm_latency in 0 100 200 400 600 800 1000 1300 1600 2000 3000 5000 # in nanoseconds
do
  for rep in 0 1 2
  do
    echo "NVM_latency=$nvm_latency, rep=$rep/3..."
    rm -rf /dev/shm/foedus_tpcc/
    rm -rf /tmp/libfoedus.*
    sleep 5 # Linux's release of shared memory has a bit of timelag.
    sudo mount -t nvmfs -o rd_delay=$nvm_latency,wr_delay=$nvm_latency,size=1000000m nvmfs /testnvm
    echo "./tpcc -warehouses=$warehouses -take_snapshot=true -nvm_folder=/testnvm -fork_workers=$fork_workers -high_priority=$high_priority -null_log_device=false -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=1 -payment_remote_percent=15 -volatile_pool_size=$volatile_pool_size -duration_micro=$duration_micro"
    ./tpcc -warehouses=$warehouses -take_snapshot=true -nvm_folder=/testnvm -fork_workers=$fork_workers -high_priority=$high_priority -null_log_device=false -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=1 -payment_remote_percent=15 -volatile_pool_size=$volatile_pool_size -duration_micro=$duration_micro &> "result_tpcc_nvm_$machine_shortname.n$nvm_latency.r$rep.log"
    sudo unmount /testnvm
  done
done
