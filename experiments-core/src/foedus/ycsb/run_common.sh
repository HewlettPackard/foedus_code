echo "FOEDUS YCSB experiments script for $machine_shortname ($machine_name)"
echo "thread_per_node=$thread_per_node, numa_nodes=$numa_nodes, snapshot_pool_size=$snapshot_pool_size, reducer_buffer_size=$reducer_buffer_size."
echo "loggers_per_node=$loggers_per_node, volatile_pool_size=$volatile_pool_size, log_buffer_mb=$log_buffer_mb, duration_micro=$duration_micro."
echo "workload=$workload, max_scan_length=$max_scan_length, read_all_fields=$read_all_fields, write_all_fields=$write_all_fields."
echo "initial_table_size=$initial_table_size, random_inserts=$random_inserts, ordered_inserts=$ordered_inserts, sort_load_keys=$sort_load_keys,"
echo "verify_loaded_data=$verify_loaded_data, rmw_additional_reads=$rmw_additional_reads, reps_per_tx=$reps_per_tx, zipfian_theta=$zipfian_theta"
echo "fork_workers=$fork_workers, null_log_device=$null_log_device, high_priority=$high_priority."

result_dir="result_ycsb_$workload_`date +%s`"

mkdir -p $result_dir
export CPUPROFILE_FREQUENCY=1 # https://code.google.com/p/gperftools/issues/detail?id=133

options="-thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -loggers_per_node=$loggers_per_node -volatile_pool_size=$volatile_pool_size -log_buffer_mb=$log_buffer_mb -duration_micro=$duration_micro -workload=$workload -max_scan_length=$max_scan_length -read_all_fields=$read_all_fields -write_all_fields=$write_all_fields -initial_table_size=$initial_table_size -random_inserts=$random_inserts -ordered_inserts=$ordered_inserts -sort_load_keys=$sort_load_keys -fork_workers=$fork_workers -verify_loaded_data=$verify_loaded_data -zipfian_theta=$zipfian_theta -rmw_additional_reads=$rmw_additional_reads -reps_per_tx=$reps_per_tx"

echo $options > $result_dir/options.txt

for rep in 1 2 3
do
  echo "masstree rep=$rep/3..."
  # be careful.
  rm -rf /dev/shm/foedus_tpcc/ &>> $result_dir/run.log
  rm -rf /tmp/libfoedus.* &>> $result_dir/run.log
  sleep 5 # Linux's release of shared memory has a bit of timelag.
  env CPUPROFILE_FREQUENCY=1 ./ycsb_masstree $options &> "$result_dir/ycsb_masstree.workload$workload.$machine_shortname.r$rep.log"
done

if [ $workload != "E" ] && [ $workload != "G" ]
then
  for rep in 1 2 3
  do
    echo "hash rep=$rep/3..."
    # be careful.
    rm -rf /dev/shm/foedus_tpcc/ &>> $result_dir/run.log
    rm -rf /tmp/libfoedus.* &>> $result_dir/run.log
    sleep 5 # Linux's release of shared memory has a bit of timelag.
    env CPUPROFILE_FREQUENCY=1 ./ycsb_hash $options &> "$result_dir/ycsb_hash.workload$workload.$machine_shortname.r$rep.log"
  done
fi
