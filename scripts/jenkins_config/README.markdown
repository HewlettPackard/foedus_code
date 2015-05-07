
Commands to extract config.xmls.

    cd /var/lib/jenkins/jobs
    ls -1 | xargs -L1 -I '{}' mkdir ~/{}
    ls -1 | xargs -L1 -I '{}' cp {}/config.xml ~/{}

To restore, copy the folders and reload configs on jenkins.


List of plugins (some of them maybe not used now. just a list of plugins on our server)

* cmake Builder
* CCCC Plug-in
* Doxygen Plugin
* Embeddable Build Status Plugin
* Git Client Plugin
* Git Plugin
* Gitlab Hook Plugin
* Gitlab Merge Request Builder
* java.io.tmpdir cleaner plugin
* SLOCCount Plugin
* Valgrind Plugin
* Warnings Plugin
* xUnit Plugin


List of dependencies specific to jenkins machine (in addition to FOEDUS's).

* jenkins
* sloccount
* valgrind (must be 3.9.0 or later on x86, 3.10.1 or later on aarch64)

Other stuffs to do on Jenkins machine.

    sudo yum install make # seriously? some cloud machine doesn't have it.
    sudo yum install yum-utils
    sudo debuginfo-install gcc gcc-c++
    sudo debuginfo-install libunwind numactl
