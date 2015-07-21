# nolog
for r in 0 1 2
do
  cd /tmp
  /dev/shm/silo/out-perf.masstree/benchmarks/dbtest --bench tpcc --num-threads 48 --scale-factor 48 --runtime 30 --numa-memory 96G --bench-opts --new-order-remote-item-pct=1 &> "/tmp/result_silo_dl580.n1.r$r.log"
done

sudo mkdir /testnvm
sudo chmod 666 /testnvm
nvm_latency=5000 # fixed to 5us

loggers=8
log_param=""
for a in $(seq $loggers)
do
   log_param="$log_param --logfile /testnvm/log$a"
done
echo "log_param=$log_param"

# withlog
for r in 0 1 2
do
  sudo mount -t nvmfs -o rd_delay_ns_fixed=$nvm_latency,wr_delay_ns_fixed=$nvm_latency,rd_delay_ns_per_kb=0,wr_delay_ns_per_kb=0,cpu_freq_mhz=2800,size=1000000m nvmfs /testnvm
  cd /testnvm
  /dev/shm/silo/out-perf.masstree/benchmarks/dbtest --bench tpcc --num-threads 48 --scale-factor 48 --runtime 30 --numa-memory 96G $log_param --bench-opts --new-order-remote-item-pct=1 &> "/tmp/result_silo_withlog_dl580.n1.r$r.log"
  cd /tmp
  sudo umount /testnvm
done
