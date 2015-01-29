# For H-store experiments.
configure_file(hstore_tpcc.properties.in hstore_tpcc.properties.in COPYONLY)
configure_file(run_hstore_common.sh run_hstore_common.sh COPYONLY)
configure_file(run_hstore_z820.sh run_hstore_z820.sh COPYONLY)
configure_file(run_hstore_dl580.sh run_hstore_dl580.sh COPYONLY)
configure_file(run_hstore_dragonhawk.sh run_hstore_dragonhawk.sh COPYONLY)

configure_file(run_hstore_withlog.sh run_hstore_withlog.sh COPYONLY)
configure_file(run_hstore_withlog_dl580.sh run_hstore_withlog_dl580.sh COPYONLY)
configure_file(run_hstore_withlog_dragonhawk.sh run_hstore_withlog_dragonhawk.sh COPYONLY)

# For H-store NVM experiments.
configure_file(hstore_tpcc_nvm.properties hstore_tpcc_nvm.properties COPYONLY)
configure_file(run_hstore_nvm.sh run_hstore_nvm.sh COPYONLY)
configure_file(run_hstore_nvm_z820.sh run_hstore_nvm_z820.sh COPYONLY)
configure_file(run_hstore_nvm_dl580.sh run_hstore_nvm_dl580.sh COPYONLY)
configure_file(run_hstore_nvm_dragonhawk.sh run_hstore_nvm_dragonhawk.sh COPYONLY)
