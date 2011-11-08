#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

import os
from job import job, log, debug, fatal, warn, run, rmtree, mktree
from vm import LinuxBenchVM, WindowsBenchVM, DATA_ROOT

rpm_vm = LinuxBenchVM('rpm', 'centos', 22)
deb_vm = LinuxBenchVM('deb', 'debian', 22)

VERSION = '2.2.0b2'
ARCHIVE = 'HTSQL-%s.tar.gz' % VERSION
PYPISRC = 'http://pypi.python.org/packages/source/H/HTSQL/%s' % ARCHIVE

@job
def pkg_deb():
    """create debian package
 
    This job creates the debian source and binary packages.
    """
    if deb_vm.missing():
       raise fatal("VM deb is not built") 
    if deb_vm.running():
       deb_vm.stop()
    if os.path.exists("build/pkg/deb"):
       rmtree("build/pkg/deb")
    deb_vm.start()
    src_path = deb_vm.download([PYPISRC])
    deb_vm.put(src_path, '/root')
    deb_vm.run("mv /root/%s /root/htsql_%s.orig.tar.gz" % (ARCHIVE, VERSION))
    deb_vm.run("tar xvfz /root/htsql_%s.orig.tar.gz -C /root" % VERSION)
    deb_vm.put(DATA_ROOT+"/pkg/debian", "/root/HTSQL-%s" % VERSION)
    deb_vm.run("cd /root/HTSQL-%s && dpkg-buildpackage" % VERSION)
    mktree("build/pkg/deb")
    deb_vm.get("/root/htsql_%s-1_all.deb" % VERSION, "build/pkg/deb")
    deb_vm.run("dpkg -i /root/htsql_%s-1_all.deb" % VERSION)
    deb_vm.run("htsql-ctl get sqlite:test.db /%27Hello%20World%27")
    deb_vm.stop()
    log()
    log("`build/pkg/deb/htsql_%s-1_all.deb` is created successfully" % VERSION)

