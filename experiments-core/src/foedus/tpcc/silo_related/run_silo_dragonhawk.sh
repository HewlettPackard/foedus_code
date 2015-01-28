# nolog
for remote_percent in 0 1 2 3 4 5 6 7 8 9 10
do
  for r in 0 1 2
  do
    cd /tmp
    /dev/shm/silo/out-perf.masstree/benchmarks/dbtest --bench tpcc --num-threads 192 --scale-factor 192 --runtime 30 --numa-memory 384G --bench-opts --new-order-remote-item-pct=$remote_percent &> "/tmp/result_silo_dh.n$remote_percent.r$r.log"
  done
done

sudo mkdir /testnvm
sudo chmod 666 /testnvm
nvm_latency=5000 # fixed to 5us

loggers=16 # 32 causes a crash in txn_proto2_impl.cc: L182. So, one per socket in this case.
# I bet it's because something goes over 512 (SILO's NMAXCORES). it's fine on DL580.
log_param=""
for a in $(seq $loggers)
do
   log_param="$log_param --logfile /testnvm/log$a"
done
echo "log_param=$log_param"


# withlog
for remote_percent in 0 1 2 3 4 5 6 7 8 9 10
do
  for r in 0 1 2
  do
    sudo mount -t nvmfs -o rd_delay_ns_fixed=$nvm_latency,wr_delay_ns_fixed=$nvm_latency,rd_delay_ns_per_kb=0,wr_delay_ns_per_kb=0,cpu_freq_mhz=2800,size=1000000m nvmfs /testnvm
    cd /testnvm
    /dev/shm/silo/out-perf.masstree/benchmarks/dbtest --bench tpcc --num-threads 192 --scale-factor 192 --runtime 30 --numa-memory 384G $log_param --bench-opts --new-order-remote-item-pct=$remote_percent &> "/tmp/result_silo_withlog_dh.n$remote_percent.r$r.log"
    cd /tmp
    sudo umount /testnvm
  done
done
