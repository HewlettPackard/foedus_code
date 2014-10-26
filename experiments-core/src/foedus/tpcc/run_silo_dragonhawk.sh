for remote_percent in 0 1 2 3 4 5 6 7 8 9 10
do
  ./benchmarks/dbtest --bench tpcc --num-threads 192 --scale-factor 192 --runtime 30 --numa-memory 384G --bench-opts --new-order-remote-item-pct=$remote_percent &> "result_silo_dh.n$remote_percent.log"
done
