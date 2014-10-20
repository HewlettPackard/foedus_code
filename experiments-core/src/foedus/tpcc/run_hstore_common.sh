echo "H-store TPC-C experiments script for $machine_shortname ($machine_name)"
echo "hstore_client_memory=$hstore_client_memory, hstore_site_memory=$hstore_site_memory, hstore_global_memory=$hstore_global_memory, hstore_client_threads_per_host=$hstore_client_threads_per_host,hstore_hosts=$hstore_hosts."

# terrible assumption, but works.
hstore_folder="/dev/shm/h-store"
cpu_affinity="true"

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
  neworder_remote_percent=$remote_percent
  payment_remote_percent=${payment_percents[$remote_percent]}
  regex1='s/PLACEHOLDER\_NEWORDER\_MULTIP\_ENABLED/'
  regex2='/g;s/PLACEHOLDER\_PAYMENT\_MULTIP\_ENABLED/'
  regex3='/g;s/PLACEHOLDER\_NEWORDER\_MULTIP\_MIX/'
  regex4='/g;s/PLACEHOLDER\_PAYMENT\_MULTIP\_MIX/'
  regex5='/g'

  if [ "$remote_percent" == "0" ]; then
    enabled='false'
  else
    enabled='true'
  fi

  regexall="$regex1$enabled$regex2$enabled$regex3$neworder_remote_percent$regex4$payment_remote_percent$regex5"
  echo "'$regexall' hstore_tpcc.properties.in"
  sed "$regexall" hstore_tpcc.properties.in > hstore_tpcc_$remote_percent.properties
  for rep in 0 1 2
  do
    echo "NewOrder Remote-percent=$neworder_remote_percent, Payment Remote-percent=$payment_remote_percent, rep=$rep/3..."
    cp -f "hstore_tpcc_$remote_percent.properties" "$hstore_folder/properties/benchmarks/tpcc.properties"
    pushd "$hstore_folder"
    ant hstore-prepare hstore-benchmark -Dproject=tpcc -Dsite.jvm_asserts=false -Dclient.blocking=false -Dsite.cpu_affinity=$cpu_affinity -Dclient.memory=$hstore_client_memory -Dsite.memory=$hstore_site_memory -Dglobal.memory=$hstore_global_memory -Dclient.txnrate=10000 -Dclient.threads_per_host=$hstore_client_threads_per_host -Dhosts="$hstore_hosts" &> "hstore_tpcc_$machine_shortname.n$remote_percent.r$rep.log"
    echo "sleeping after experiments..."
    sleep 10
    echo "woke up."
    popd
  done
done
