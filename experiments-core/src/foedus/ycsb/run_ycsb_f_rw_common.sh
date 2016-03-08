. config_ycsb.sh "F"
extra_table_size=0
extended_rw_lock=1
parallel_lock=0
force_canonical_xlocks_in_precommit=1

for hot_threshold in 0 10 126 256; do
  for initial_table_size in 50 100 1000 10000 100000 1000000; do
    for enable_retrospective_lock_list in 0 1; do
      for rmw_additional_reads in 0 1 2 3 4 5 6 7 8 9 10; do
        reps_per_tx=`expr 10 - $rmw_additional_reads`
        . run_common.sh
      done
    done
  done
done

