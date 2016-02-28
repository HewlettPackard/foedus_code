for hot_threshold in 0 10 126 256; do
  for size in 50 100 1000 10000 100000 1000000; do
    for theta in 0; do
      for addread in 0 1 2 3 4 5 6 7 8 9 10; do
        . config_ycsb_f.sh $theta $addread `expr 10 - $addread` $size $hot_threshold
        . run_common.sh
      done
    done
  done
done
