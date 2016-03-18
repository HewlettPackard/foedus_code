echo "FOEDUS YCSB shifting experiments script for $machine_shortname ($machine_name)"
echo "thread_per_node=$thread_per_node, numa_nodes=$numa_nodes"
echo "loggers_per_node=$loggers_per_node, log_buffer_mb=$log_buffer_mb"

echo "hot_threshold=$hot_threshold"
result_dir="result_ycsb_$workload_`date +%s`"

mkdir -p $result_dir
export CPUPROFILE_FREQUENCY=1 # https://code.google.com/p/gperftools/issues/detail?id=133

# the followings are irrelevant/fixed in this experiment.
snapshot_pool_size=1
reducer_buffer_size=1
loggers_per_node=1
log_buffer_mb=64
workload=F
force_canonical_xlocks_in_precommit=1
max_scan_length=1000
read_all_fields=false
write_all_fields=false
random_inserts=false
ordered_inserts=true
sort_load_keys=true
fork_workers=true
verify_loaded_data=false
zipfian_theta=0
null_log_device=true
extended_rw_lock=true
enable_retrospective_lock_list=true
parallel_lock=false

# we might adjust the following
sort_keys=false
duration_micro=800000
rmw_additional_reads=20
reps_per_tx=0
extra_table_reads=1
extra_table_rmws=0
extra_table_size=1
initial_table_size=1000000

for hot_threshold in 0 1 2 4 8 10 13 20 25
do
  options="-thread_per_node=$thread_per_node -numa_nodes=$numa_nodes -snapshot_pool_size=$snapshot_pool_size -reducer_buffer_size=$reducer_buffer_size -loggers_per_node=$loggers_per_node -volatile_pool_size=$volatile_pool_size -log_buffer_mb=$log_buffer_mb -duration_micro=$duration_micro -workload=$workload -max_scan_length=$max_scan_length -read_all_fields=$read_all_fields -write_all_fields=$write_all_fields -initial_table_size=$initial_table_size -random_inserts=$random_inserts -ordered_inserts=$ordered_inserts -sort_load_keys=$sort_load_keys -fork_workers=$fork_workers -verify_loaded_data=$verify_loaded_data -zipfian_theta=$zipfian_theta -rmw_additional_reads=$rmw_additional_reads -reps_per_tx=$reps_per_tx -null_log_device=$null_log_device -hot_threshold=$hot_threshold -sort_keys=$sort_keys -extended_rw_lock=$extended_rw_lock -enable_retrospective_lock_list=$enable_retrospective_lock_list -parallel_lock=$parallel_lock -extra_table_size=$extra_table_size -extra_table_rmws=$extra_table_rmws -extra_table_reads=$extra_table_reads -shifting_workload=true"
  # This overwrites the options.txt each time, but not a big issue
  echo $options > $result_dir/options.txt

  #echo "masstree h=$hot_threshold..."
  #rm -rf /dev/shm/foedus_tpcc/ &>> $result_dir/run.log
  #rm -rf /tmp/libfoedus.* &>> $result_dir/run.log
  #sleep 5 # Linux's release of shared memory has a bit of timelag.
  #env CPUPROFILE_FREQUENCY=1 ./ycsb_masstree $options &> "$result_dir/ycsb_masstree.$machine_shortname.h$hot_threshold.log"
  #mv bucketed_throughputs.tsv "$result_dir/bucketed_throughputs_masstree_h$hot_threshold.tsv"

  echo "hash h=$hot_threshold..."
  rm -rf /dev/shm/foedus_tpcc/ &>> $result_dir/run.log
  rm -rf /tmp/libfoedus.* &>> $result_dir/run.log
  sleep 5 # Linux's release of shared memory has a bit of timelag.
  env CPUPROFILE_FREQUENCY=1 ./ycsb_hash $options &> "$result_dir/ycsb_hash.$machine_shortname.h$hot_threshold.log"
  mv bucketed_throughputs.tsv "$result_dir/bucketed_throughputs_hash_h$hot_threshold.tsv"
  mv bucketed_aborts.tsv "$result_dir/bucketed_aborts_hash_h$hot_threshold.tsv"
  mv bucketed_accumulated.tsv "$result_dir/bucketed_accumulated_hash_h$hot_threshold.tsv"
done

