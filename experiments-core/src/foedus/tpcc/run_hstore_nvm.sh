echo "H-store TPC-C experiments with Anticaching script for $machine_shortname ($machine_name)"
echo "hstore_client_memory=$hstore_client_memory, hstore_site_memory=$hstore_site_memory, hstore_global_memory=$hstore_global_memory, hstore_client_threads_per_host=$hstore_client_threads_per_host,hstore_hosts=$hstore_hosts."

# terrible assumption, but works.
hstore_folder="/dev/shm/h-store"
nvm_folder="/testnvm"
cpu_affinity="true"

cp -f "hstore_tpcc_nvm.properties" "$hstore_folder/properties/benchmarks/tpcc.properties"
for nvm_latency in 0 100 200 400 600 800 1000 1300 1600 2000 3000 5000 # in nanoseconds
do
  for rep in 0 1 2
  do
    echo "NVM Latency=$nvm_latency, rep=$rep/3..."
    sudo mount -t nvmfs -o rd_delay_ns_fixed=$nvm_latency,wr_delay_ns_fixed=$nvm_latency,rd_delay_ns_per_kb=0,wr_delay_ns_per_kb=0,cpu_freq_mhz=2800,size=1000000m nvmfs /testnvm
    rm -rf "$nvm_folder/anticache"
    rm -rf "$nvm_folder/commandlog"
    mkdir "$nvm_folder/anticache"
    mkdir "$nvm_folder/commandlog"
    pushd "$hstore_folder"
    ant hstore-prepare -Dproject=tpcc -Devictable="HISTORY,CUSTOMER,ORDERS,ORDER_LINE" -Dhosts="$hstore_hosts" &> "hstore_tpcc_nvm_prepare_$machine_shortname.n$nvm_latency.r$rep.log"
    ant hstore-benchmark -Dproject=tpcc -Dsite.anticache_enable=true -Dsite.anticache_dir=$nvm_folder/anticache -Dsite.commandlog_enable=true -Dsite.commandlog_dir="$nvm_folder/commandlog/" -Dsite.commandlog_timeout=500 -Dsite.jvm_asserts=false -Dclient.blocking=false -Dsite.cpu_affinity=$cpu_affinity -Dclient.memory=$hstore_client_memory -Dsite.memory=$hstore_site_memory -Dglobal.memory=$hstore_global_memory -Dclient.txnrate=2000 -Dclient.threads_per_host=$hstore_client_threads_per_host -Dhosts="$hstore_hosts" &> "hstore_tpcc_nvm_$machine_shortname.n$nvm_latency.r$rep.log"
    echo "sleeping after experiments..."
    sleep 10
    sudo umount /testnvm
    echo "woke up."
    popd
  done
done
