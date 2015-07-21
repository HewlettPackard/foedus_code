hstore_hosts="localhost:0:0-7;localhost:1:8-15;localhost:2:16-23;localhost:3:24-31"
hstore_client_memory="2048"
hstore_site_memory="30720"
hstore_global_memory="20480"
hstore_client_threads_per_host="40" # 100~ are too large to handle for this throughput...
machine_name="DL580"
machine_shortname="dl580"
. run_hstore_nvm.sh
