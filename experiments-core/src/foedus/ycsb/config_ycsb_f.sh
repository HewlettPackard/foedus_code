#!/bin/bash
# $1 - zipfian_theta
# $2 - rmw_additional_reads
# $3 - reps_per_tx

workload=F
max_scan_length=1000
read_all_fields=0
write_all_fields=0
initial_table_size=1000000
random_inserts=0
use_string_keys=1
verify_loaded_data=0
zipfian_theta=$1
rmw_additional_reads=$2
reps_per_tx=$3
ordered_inserts=0
sort_load_keys=0
