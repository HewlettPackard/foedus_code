for remote_percent in 0 1 2 3 4 5 6 7 8 9 10
do
  ./benchmarks/dbtest --bench tpcc --num-threads 48 --scale-factor 48 --runtime 30 --numa-memory 96G --bench-opts --new-order-remote-item-pct=$remote_percent &> "result_silo_dl580.n$remote_percent.log"
done
