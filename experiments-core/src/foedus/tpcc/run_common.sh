echo "FOEDUS TPC-C experiments script for $machine_shortname ($machine_name)"
echo "warehouses=$warehouses, loggers_per_node=$loggers_per_node, volatile_pool_size=$volatile_pool_size, duration_micro=$duration_micro."

payment_percents[0]=0
payment_percents[1]=15
payment_percents[2]=30
payment_percents[3]=45
payment_percents[4]=60
payment_percents[5]=75
payment_percents[6]=90
payment_percents[7]=100
payment_percents[8]=100
payment_percents[9]=100
payment_percents[10]=100

for remote_percent in 0 1 2 3 4 5 6 7 8 9 10
do
  for rep in 0 1 2
  do
    neworder_remote_percent=$remote_percent
    payment_remote_percent=${payment_percents[$remote_percent]}
    echo "NewOrder Remote-percent=$neworder_remote_percent, Payment Remote-percent=$payment_remote_percent, rep=$rep/3..."
    # be careful.
    rm -rf /dev/shm/foedus_tpcc/
    rm -rf /tmp/libfoedus.*
    ./tpcc -warehouses=$warehouses -ignore_volatile_size_warning=true -loggers_per_node=$loggers_per_node -thread_per_node=$thread_per_node -log_buffer_mb=$log_buffer_mb -neworder_remote_percent=$neworder_remote_percent -payment_remote_percent=$payment_remote_percent -volatile_pool_size=$volatile_pool_size -duration_micro=$duration_micro &> "result_tpcc_$machine_shortname.n$remote_percent.r$rep.log"
  done
done
