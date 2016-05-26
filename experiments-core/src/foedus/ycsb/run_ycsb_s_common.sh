. config_ycsb.sh "S"
extra_table_size=1000
extra_table_reads=0
extra_table_rmws=0
extended_rw_lock=1
force_canonical_xlocks_in_precommit=1
initial_table_size=1
reps_per_tx=1
rmw_additional_reads=0
enable_retrospective_lock_list=1

# 123 is a special value for [Thomas98]
for hot_threshold in 0 10 123 126; do
  . run_common.sh
done

