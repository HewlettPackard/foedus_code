# This one is just for testing.
# OLAP experiments requires huge memory, so we can't run it full power in Z820.
# heh, I thought 128GB is too much when I got the machine.
warehouses=4
loggers_per_node=2
volatile_pool_size=12
snapshot_pool_size=1
reducer_buffer_size=2
duration_micro=30000000
thread_per_node=2
numa_nodes=2
log_buffer_mb=1024
machine_name="Hideaki's Z820"
machine_shortname="z820"
. run_olap.sh
