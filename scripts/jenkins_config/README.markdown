
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
    sudo yum install perl-Algo* perl-Reg*
    sudo debuginfo-install gcc gcc-c++
    sudo debuginfo-install libunwind numactl
    sudo sh -c  'echo never > /sys/kernel/mm/transparent_hugepage/enabled'
    sudo sh -c 'echo 15000 > /proc/sys/vm/nr_hugepages'
    Modify /etc/sysctl.conf
    Modify /etc/security/limits.conf

**Important** notes for Jenkins's ulimit on Ubuntu/Debian:

    sudo emacs -nw /etc/default/jenkins

Then, find this line:

    # OS LIMITS SETUP
    #   comment this out to observe /etc/security/limits.conf
    #   this is on by default because http://github.com/jenkinsci/jenkins/commit/2fb288474e980d0e7ff9c4a3b768874835a3e92e
    #   reported that Ubuntu's PAM configuration doesn't include pam_limits.so, and as a result the # of file
    #   descriptors are forced to 1024 regardless of /etc/security/limits.conf
    MAXOPENFILES=8192

Change it to:

    MAXOPENFILES=1000000
    MAXMEMLOCK=unlimited

Next,

    sudo emacs -nw /etc/init.d/jenkins

Find this line:

    # If the var MAXOPENFILES is enabled in /etc/default/jenkins then set the max open files to the
    # proper value
    if [ -n "$MAXOPENFILES" ]; then
        [ "$VERBOSE" != no ] && echo Setting up max open files limit to $MAXOPENFILES
        ulimit -n $MAXOPENFILES
    fi

Add the following next to it:

    # Same for memlock. I really wish trunk Jenkins adds this as a builtin feature.
    if [ -n "MAXMEMLOCK" ]; then
        [ "$VERBOSE" != no ] && echo Setting up memlock limit to $MAXMEMLOCK
        ulimit -l $MAXMEMLOCK
    fi

We need this in addition to modifying /etc/security/limits.conf because:

    EngineOptions::prescreen_ulimits() {
    ...
    // Record of a struggle: WTF,, no idea why, but I'm seeing an weird behavior only on Ubuntu.
    // I did set limits.conf, and ulimit -n is saying 100000, but the above code returns "8192"
    // on Ubuntu. As a tentative solution, reduced the min value to 8192.
    // This happens only when I run the code as jenkins user from jenkins service.
    // If I run it as myself, or "sudo su jenkins" then run it, it runs fine. WWWTTTTFFF.

    // 2015 Jun: Ahhh, I got it. It's because jenkins service is started by a daemon script:
    // http://blog.mindfab.net/2013/12/changing-ulimits-for-jenkins-daemons.html
    //  "The important part is that you have to specify the ulimits, e.g., for the number
    //   of open files before start-stop-daemon is called. The reason is that
    //   **start-stop-daemon doesn't consider pam**
    //   and hence will not find the limits which have been specified in /etc/security/limits.conf."

To confirm whether this issue is happening, create a jenkins job that just runs ulimit -a.
Check for "locked memory(kbytes)" and "nofiles".
For some reason, only on Ubuntu the values are default values, not the ones in limits.conf.
This issue doesn't happen on Fedora.

Cron jobs to clean logs.

    # Clean file and dirs more than 3 days old in /dev/shm/foedus_test/glog nightly
    /usr/bin/find /dev/shm/foedus_test/glog -type d -mtime +2 -exec /bin/rm -rf '{}' \;


Notes on Ubuntu's shmget issue.

    sudo su
    groupadd hugeshm
    usermod -a -G hugeshm jenkins
    id jenkins

    # suppose the group ID is 1001
    echo 1001 > /proc/sys/vm/hugetlb_shm_group
    # Add "vm.hugetlb_shm_group=1001" entry in sysctl.conf.

This was the trick! Weird. This wasn't requied on Fedora, or Ubuntu 1404 on aarch64.
Maybe kernel version? FC21: 3.17.4, UB1504: 3.19.0


== Setting up cihead/centos7/ciub1404/etc on the new cluster

    cihead: http://cihead.labs.hpe.com/
    centos7.2 x86_64 (cicentos7.labs.hpe.com): http://cihead.labs.hpe.com/centos7
    ubuntu1404 x86_64 (ciub1404.labs.hpe.com): http://cihead.labs.hpe.com/ub1404

    https://wiki.jenkins-ci.org/display/JENKINS/Running+Jenkins+behind+Apache

    sudo emacs -nw /etc/sysconfig/jenkins
    # on each slave,
    # JENKINS_ARGS=" --prefix=/<slave_name>", for example:
    JENKINS_ARGS="--prefix=/centos7"

Hey, seems like cmakebuilder 2.4 is a lot different from 1.9.
Now I have to redo all config.xml. duhhh.

On centos7, get cloc from here: http://pkgs.org/centos-6/epel-i386/cloc-1.58-4.el6.noarch.rpm.html
I don't know why, but EPEL didn't have it on the machine.

At least on cicentos7, I had to do:

    sudo chmod -R 777 /tmp/foedus_test /dev/shm/foedus_test

Otherwise the create_tmp_dir testcase fails with permission errors.

