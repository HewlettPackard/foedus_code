. config_ycsb.sh "F"
extra_table_size=0
extended_rw_lock=1
parallel_lock=0
force_canonical_xlocks_in_precommit=1

rmw_additional_reads=10
reps_per_tx=0
enable_retrospective_lock_list=0
for hot_threshold in 0 10 126 256; do
  for initial_table_size in 50 100 1000 10000 100000 1000000; do
    . run_common.sh
  done
done

