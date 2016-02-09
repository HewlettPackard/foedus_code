#!/bin/bash
# $1 - results dir
# 16x12 threads, table size = 1000 or 10000, varying hot_threshold and xRMW+yR
dir=$1
initial_table_size=$2

echo -ne "Threshold"
for R in 0; do # 2 4 6 8 10; do
    echo -ne ,$R"R+"`expr 10 - $R`"RMW"
done
echo

for hot_threshold in 0 50 100 256; do
    echo -ne $hot_threshold
    for R in 10; do #  2 4 6 8 10; do
        all_files=`ls $dir/*/options.txt`
        opfile=`grep "initial_table_size=$initial_table_size " $all_files | \
            grep "rmw_additional_reads=$R " | \
            grep "hot_threshold=$hot_threshold " | \
            cut -d ':' -f1`
        total=0
        for iter in 1 2 3; do
            file=`dirname $opfile`/ycsb_masstree.workloadF.lp.r$iter.log
            MTPS=`grep final $file | tr '>' '\n'| grep MTPS | cut -d '<' -f1 | tr -d '\n'`
            total=`echo "$MTPS + $total" | bc -l`
        done
        MTPS=`echo "scale=3; $total / 3" | bc -l`
        MTPS=`echo "if ($MTPS < 1) print 0; $MTPS" | bc`
        echo -ne ,$MTPS
    done
    echo
done
