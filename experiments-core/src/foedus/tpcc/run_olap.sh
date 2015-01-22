echo "FOEDUS TPC-C OLAP experiments script for $machine_shortname ($machine_name)"

# This script doesn't automatically change MAX_OL_CNT.
# Edit it to 15/100/500 manually, compile, and run this script.
# Do not forget to rename result log files to name MAX_OL_CNT.
# Just 3 times, men.

null_log_device=false
high_priority=false
fork_workers=true

for rep in 0 1 2
do
  echo "Volatile experiment. rep=$rep/3..."
  # be careful.
  rm -rf /dev/shm/foedus_tpcc/
  rm -rf /tmp/libfoedus.*
  sleep 5 # Linux's release of shared memory has a bit of timelag.
  export CPUPROFILE_FREQUENCY=1 # https://code.google.com/p/gperftools/issues/detail?id=133
  env CPUPROFILE_FREQUENCY=1 ./tpcc_olap -warehouses=$warehouses -take_snapshot=false -fork_workers=$fork_workers -nvm_folder=/dev/shm -high_priority=$high_priority -null_log_device=$null_log_device -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=1 -payment_remote_percent=15 -volatile_pool_size=$volatile_pool_size -snapshot_pool_size=2 -reducer_buffer_size=2 -duration_micro=$duration_micro &> "result_tpcc_olap_vol_$machine_shortname.r$rep.log"
done


nvm_latency=5000
sudo mkdir /testnvm
sudo chmod 666 /testnvm
for rep in 0 1 2
do
  echo "NVM experiment. rep=$rep/3..."
  # be careful.
  rm -rf /dev/shm/foedus_tpcc/
  rm -rf /tmp/libfoedus.*
  sleep 5 # Linux's release of shared memory has a bit of timelag.
  export CPUPROFILE_FREQUENCY=1 # https://code.google.com/p/gperftools/issues/detail?id=133
  sudo mount -t nvmfs -o rd_delay_ns_fixed=$nvm_latency,wr_delay_ns_fixed=$nvm_latency,rd_delay_ns_per_kb=0,wr_delay_ns_per_kb=0,cpu_freq_mhz=2800,size=1000000m nvmfs /testnvm
  echo "./tpcc_olap -warehouses=$warehouses -fork_workers=$fork_workers -nvm_folder=/dev/shm -high_priority=$high_priority -null_log_device=$null_log_device -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=1 -payment_remote_percent=15 -volatile_pool_size=$volatile_pool_size -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -duration_micro=$duration_micro"
  env CPUPROFILE_FREQUENCY=1 ./tpcc_olap -warehouses=$warehouses -take_snapshot=true -fork_workers=$fork_workers -nvm_folder=/testnvm -high_priority=$high_priority -null_log_device=$null_log_device -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=1 -payment_remote_percent=15 -volatile_pool_size=$volatile_pool_size -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -duration_micro=$duration_micro &> "result_tpcc_olap_nvm_$machine_shortname.r$rep.log"
  sudo umount /testnvm
done
