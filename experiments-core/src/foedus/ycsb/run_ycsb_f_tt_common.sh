. config_ycsb.sh "F"
extended_rw_lock=1
parallel_lock=0
force_canonical_xlocks_in_precommit=1
initial_table_size=1000000

# records to access in user_table
normal_accesses=(0 5 10 20 50)

# records to access in extra_table
extra_accesses=(1 2 3 4)

# send rmws to user_table
for hot_threshold in 0 10 256; do # 126 256; do
  for enable_retrospective_lock_list in 0 1; do
    for rmw in $normal_accesses; do
      reps_per_tx=$rmw
      for reads in $extra_accesses; do
        extra_table_size=$reads
        extra_table_reads=$reads
        extra_table_rmws=0
        . run_common.sh
      done
    done
  done
done

# send rmws to extra_table
for hot_threshold in 0 10 256; do # 126 256; do
  for enable_retrospective_lock_list in 0 1; do
    for rmw in $extra_accesses; do
      extra_table_size=$rmw
      extra_table_reads=0
      extra_table_rmws=$rmw
      reps_per_tx=0
      for reads in $normal_accesses; do
        rmw_additional_reads=$reads
        . run_common.sh
      done
    done
  done
done
